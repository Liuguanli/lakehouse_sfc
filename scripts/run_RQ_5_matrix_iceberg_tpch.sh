#!/usr/bin/env bash
set -euo pipefail

# RQ5 matrix (Iceberg): run RQ1-style specs on Iceberg layouts for TPCH and Amazon.
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
# Scenarios (Iceberg) – mirror RQ1 TPCH scenarios; loader fields kept for parity
# Layouts limited to baseline/linear/zorder for Iceberg.

declare -A SCENARIO_DEFAULT=(
  [name]="SCENARIO_DEFAULT"
  [layouts]="baseline,linear,zorder"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_receiptdate"
  [target_mb]="128"
  [skip_load]=1
  [query_args]="--spec-dir workload_spec/tpch_rq1 --spec-glob spec_tpch_RQ1_*_C1_N2_O1.yaml"
  [output_root]="workloads/tpch_rq5_iceberg/SCENARIO_DEFAULT"
)

declare -A SCENARIO_DEFAULT_V1=(
  [name]="SCENARIO_DEFAULT_V1"
  [layouts]="baseline,linear,zorder"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_receiptdate,l_shipdate"
  [target_mb]="128"
  [query_args]="--spec-dir workload_spec/tpch_rq1 --spec-glob spec_tpch_RQ1_*_C1_N2_O1.yaml"
  [output_root]="workloads/tpch_rq5_iceberg/SCENARIO_DEFAULT_V1"
)

declare -A SCENARIO_O2_V1=(
  [name]="SCENARIO_O2_V1"
  [layouts]="baseline,linear,zorder"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_commitdate,l_suppkey"
  [target_mb]="128"
  [query_args]="--spec-dir workload_spec/tpch_rq1 --spec-glob spec_tpch_RQ1_*_C1_N2_O1.yaml"
  [output_root]="workloads/tpch_rq5_iceberg/SCENARIO_O2_V1"
)

declare -A SCENARIO_O2_V2=(
  [name]="SCENARIO_O2_V2"
  [layouts]="baseline,linear,zorder"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_suppkey,l_commitdate"
  [target_mb]="128"
  [query_args]="--spec-dir workload_spec/tpch_rq1 --spec-glob spec_tpch_RQ1_*_C1_N2_O1.yaml"
  [output_root]="workloads/tpch_rq5_iceberg/SCENARIO_O2_V2"
)

declare -A SCENARIO_O3_V1=(
  [name]="SCENARIO_O3_V1"
  [layouts]="baseline,linear,zorder"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_orderkey,l_suppkey"
  [target_mb]="128"
  [query_args]="--spec-dir workload_spec/tpch_rq1 --spec-glob spec_tpch_RQ1_*_C1_N2_O1.yaml"
  [output_root]="workloads/tpch_rq5_iceberg/SCENARIO_O3_V1"
)

declare -A SCENARIO_O3_V2=(
  [name]="SCENARIO_O3_V2"
  [layouts]="baseline,linear,zorder"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_suppkey,l_orderkey"
  [target_mb]="128"
  [query_args]="--spec-dir workload_spec/tpch_rq1 --spec-glob spec_tpch_RQ1_*_C1_N2_O1.yaml"
  [output_root]="workloads/tpch_rq5_iceberg/SCENARIO_O3_V2"
)

declare -A SCENARIO_O4_V1=(
  [name]="SCENARIO_O4_V1"
  [layouts]="baseline,linear,zorder"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_extendedprice,l_quantity"
  [target_mb]="128"
  [query_args]="--spec-dir workload_spec/tpch_rq1 --spec-glob spec_tpch_RQ1_*_C1_N2_O1.yaml"
  [output_root]="workloads/tpch_rq5_iceberg/SCENARIO_O4_V1"
)

declare -A SCENARIO_O4_V2=(
  [name]="SCENARIO_O4_V2"
  [layouts]="baseline,linear,zorder"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_quantity,l_extendedprice"
  [target_mb]="128"
  [query_args]="--spec-dir workload_spec/tpch_rq1 --spec-glob spec_tpch_RQ1_*_C1_N2_O1.yaml"
  [output_root]="workloads/tpch_rq5_iceberg/SCENARIO_O4_V2"
)

SCENARIOS=(SCENARIO_DEFAULT SCENARIO_DEFAULT_V1 SCENARIO_O2_V1 SCENARIO_O2_V2 SCENARIO_O3_V1 SCENARIO_O3_V2 SCENARIO_O4_V1 SCENARIO_O4_V2)

# ---------------------------------------------------------------------------

for scenario_var in "${SCENARIOS[@]}"; do
  declare -n scenario="$scenario_var"
  name="${scenario[name]:-scenario}"
  layouts="${scenario[layouts]:-baseline,linear,zorder}"
  dataset_name="${scenario[dataset_name]:-tpch_16}"
  output_root="${scenario[output_root]:-${ROOT_DIR}/workloads/rq5_iceberg/${name}}"
  query_args_str="${scenario[query_args]:---spec-dir workload_spec/tpch_rq1 --spec-glob spec_tpch_RQ1_*.yaml}"

  echo "===== Running scenario: $name (Iceberg) ====="

  cmd=( "$RUN_SCRIPT"
        --workload-type "tpch"
        --dataset-name "$dataset_name"
        --output-root "$output_root"
        --iceberg
        --iceberg-layouts "$layouts"
        --tag "scenario=${name}" )

  read -r -a query_args <<<"$query_args_str"
  if [[ ${#query_args[@]} -gt 0 ]]; then
    cmd+=("${query_args[@]}")
  fi

  ICEBERG_LAYOUTS="$layouts" bash "${cmd[@]}"

  clean_spark_eventlogs
done

echo "[DONE] Iceberg RQ5 scenarios completed."
