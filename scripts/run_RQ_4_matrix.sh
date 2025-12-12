#!/usr/bin/env bash
set -euo pipefail

# RQ4 matrix runner: execute generated TPCH RQ4 specs (group/order/limit) on a
# selected set of layouts. Delegates to scripts/run_RQ_1.sh with custom query args.

[[ -f "${HOME}/.lakehouse/env" ]] && source "${HOME}/.lakehouse/env"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_SCRIPT="${RQ_MATRIX_RUN_SCRIPT:-${ROOT_DIR}/scripts/run_RQ_1.sh}"
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
# Scenario definitions
# Each associative array may set:
#   name         : label for logging/output directories
#   scales       : scale factors (space/comma separated)
#   layouts      : HUDI layouts (default no_layout,linear,zorder,hilbert)
#   query_args   : extra args passed to run_RQ_query (quoted string)
#   start_after  : --start-after passed to run_RQ_query
#   skip_load    : set to 1 to skip load stage
#   skip_query   : set to 1 to skip query stage
#   output_root  : override query output root
# ---------------------------------------------------------------------------


declare -A SCENARIO_RQ4_O1_V1=(
  [name]="RQ4_O1_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_receiptdate"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq4 --spec-glob spec_tpch_RQ4_*.yaml"
  [output_root]="workloads/tpch_rq4"
  [skip_load]=0
  [skip_query]=0
)

declare -A SCENARIO_RQ4_O2_V1=(
  [name]="RQ4_O2_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_commitdate,l_suppkey"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq4 --spec-glob spec_tpch_RQ4_*.yaml"
  [output_root]="workloads/tpch_rq4"
  [skip_load]=0
  [skip_query]=0
)

declare -A SCENARIO_RQ4_O3_V1=(
  [name]="RQ4_O3_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_orderkey,l_suppkey"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq4 --spec-glob spec_tpch_RQ4_*.yaml"
  [output_root]="workloads/tpch_rq4"
  [skip_load]=0
  [skip_query]=0
)


declare -A SCENARIO_RQ4_O4_V1=(
  [name]="RQ4_O4_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_extendedprice,l_quantity"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq4 --spec-glob spec_tpch_RQ4_*.yaml"
  [output_root]="workloads/tpch_rq4"
  [skip_load]=0
  [skip_query]=0
)


declare -A SCENARIO_RQ4_O5_V1=(
  [name]="RQ4_O5_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_receiptdate,l_commitdate"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq4 --spec-glob spec_tpch_RQ4_*.yaml"
  [output_root]="workloads/tpch_rq4"
  [skip_load]=0
  [skip_query]=0
)

declare -A SCENARIO_RQ4_O6_V1=(
  [name]="RQ4_O6_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_extendedprice,l_quantity,l_shipdate"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq4 --spec-glob spec_tpch_RQ4_*.yaml"
  [output_root]="workloads/tpch_rq4"
  [skip_load]=0
  [skip_query]=0
)

declare -A SCENARIO_RQ4_O7_V1=(
  [name]="RQ4_O7_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_suppkey,l_shipdate,l_extendedprice,l_quantity"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq4 --spec-glob spec_tpch_RQ4_*.yaml"
  [output_root]="workloads/tpch_rq4"
  [skip_load]=0
  [skip_query]=0
)

declare -A SCENARIO_RQ4_O8_V1=(
  [name]="RQ4_O8_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_commitdate,l_suppkey,l_extendedprice,l_quantity"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq4 --spec-glob spec_tpch_RQ4_*.yaml"
  [output_root]="workloads/tpch_rq4"
  [skip_load]=0
  [skip_query]=0
)

SCENARIOS=(
  SCENARIO_RQ4_O1_V1
  SCENARIO_RQ4_O2_V1
  SCENARIO_RQ4_O3_V1
  SCENARIO_RQ4_O4_V1
  SCENARIO_RQ4_O5_V1
  SCENARIO_RQ4_O6_V1
  SCENARIO_RQ4_O7_V1
  SCENARIO_RQ4_O8_V1
)

# ---------------------------------------------------------------------------

for scenario_var in "${SCENARIOS[@]}"; do
  declare -n scenario="$scenario_var"
  name="${scenario[name]}"
  [[ -n "$name" ]] || name="scenario"
  echo "===== Running RQ4 scenario: $name ====="

  scales_raw="${scenario[scales]:-16}"
  scales_raw="${scales_raw//,/ }"
  read -r -a scales <<<"$scales_raw"
  if [[ ${#scales[@]} -eq 0 ]]; then
    echo "[WARN] No scales defined for scenario=${name}, skipping."
    continue
  fi

  for scale in "${scales[@]}"; do
    scale="${scale// /}"
    [[ -n "$scale" ]] || continue

    dataset_name="tpch_${scale}"
    stats_file="${ROOT_DIR}/workloads/stats/tpch_${scale}_stats.yaml"
    output_root="${scenario[output_root]:-${ROOT_DIR}/workloads/tpch_rq4}"
    output_root="${output_root%/}/scale_${scale}/${name}"

    if [[ ! -f "$stats_file" ]]; then
      echo "[ERROR] Stats file missing for scale=${scale}: $stats_file" >&2
      exit 1
    fi

    cmd=( "$RUN_SCRIPT"
          --scales "$scale"
          --layouts "${scenario[layouts]:-no_layout,linear,zorder,hilbert}"
          --dataset tpch
          --dataset-name "$dataset_name"
          --output-root "$output_root"
          --tag "scenario=${name}"
          --tag "scale=${scale}" )

    [[ -n ${scenario[record_key]:-} ]] && cmd+=(--record-key "${scenario[record_key]}")
    [[ -n ${scenario[precombine]:-} ]] && cmd+=(--precombine-field "${scenario[precombine]}")
    [[ -n ${scenario[partition]:-} ]] && cmd+=(--partition-field "${scenario[partition]}")
    [[ -n ${scenario[sort]:-} ]] && cmd+=(--sort-columns "${scenario[sort]}")
    if [[ ${scenario[skip_load]:-0} -eq 1 ]]; then
      cmd+=(--skip-load)
    fi
    if [[ ${scenario[skip_query]:-0} -eq 1 ]]; then
      cmd+=(--skip-query)
    fi

    query_args_str="${scenario[query_args]:---workload-type custom --spec-dir workload_spec/tpch_rq4 --spec-glob spec_tpch_RQ4_*.yaml}"
    query_args_str+=" --stats-file ${stats_file}"
    if [[ -n ${scenario[start_after]:-} ]]; then
      query_args_str+=" --start-after ${scenario[start_after]}"
      scenario[start_after]=""
    fi
    read -r -a query_args <<<"$query_args_str"
    cmd+=(-- "${query_args[@]}")

    HUDI_LAYOUTS="${scenario[layouts]:-no_layout,linear,zorder,hilbert}" \
      bash "${cmd[@]}"

    if [[ ${scenario[skip_load]:-0} -eq 0 ]]; then
      echo "[CLEAN] Removing generated data for scale=${scale}"
      bash "${ROOT_DIR}/scripts/clean_data.sh" --yes --scales "$scale"
    else
      echo "[SKIP CLEAN] skip_load enabled, retaining data for scale=${scale}"
    fi

    clean_spark_eventlogs
  done
done

echo "[DONE] All RQ4 scenarios completed."
