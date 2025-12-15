#!/usr/bin/env bash
set -euo pipefail

# Scenario runner for RQ3 (data cardinality vs. layout). Define one or more
# scenarios below; this script loops scales per scenario and invokes
# scripts/run_RQ_1.sh for each scale with the appropriate stats file.

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
#   scales       : comma/space separated list of scale factors
#   layouts      : layout list (default no_layout,linear,zorder,hilbert)
#   query_args   : extra args passed to run_RQ_query (quoted string)
#   record_key   : loader RECORD_KEY override
#   precombine   : PRECOMBINE_FIELD override
#   partition    : PARTITION_FIELD override
#   sort         : SORT_COLUMNS override
#   target_mb    : TARGET_FILE_MB override
#   start_after  : --start-after argument passed to run_RQ_query
#   skip_load    : set to 1 to skip load stage
#   skip_query   : set to 1 to skip query stage
# ---------------------------------------------------------------------------

# declare -A SCENARIO_RQ3_DEFAULT=(
#   [name]="RQ3_DEFAULT"
#   [scales]="1,4,64"
#   [layouts]="no_layout,linear,zorder,hilbert"
#   [record_key]="l_orderkey,l_linenumber"
#   [precombine]="l_commitdate"
#   [partition]="l_returnflag,l_linestatus"
#   [sort]="l_shipdate,l_receiptdate"
#   [target_mb]="128"
#   [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq3 --spec-glob spec_tpch_*.yaml"
#   # [start_after]="spec_tpch_RQ3_Q3.yaml"
#   [skip_load]=0
#   [skip_query]=0
# )

# declare -A SCENARIO_RQ3_O2_V1=(
#   [name]="RQ3_O2_V1"
#   [scales]="1,4,64"
#   [layouts]="no_layout,linear,zorder,hilbert"
#   [record_key]="l_orderkey,l_linenumber"
#   [precombine]="l_commitdate"
#   [partition]="l_returnflag,l_linestatus"
#   [sort]="l_commitdate,l_suppkey"
#   [target_mb]="128"
#   [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq3 --spec-glob spec_tpch_*.yaml"
#   # [start_after]="spec_tpch_RQ3_Q3.yaml"
#   [skip_load]=0
#   [skip_query]=0
# )

# declare -A SCENARIO_RQ3_O2_V2=(
#   [name]="RQ3_O2_V2"
#   [scales]="1,4,64"
#   [layouts]="no_layout,linear,zorder,hilbert"
#   [record_key]="l_orderkey,l_linenumber"
#   [precombine]="l_commitdate"
#   [partition]="l_returnflag,l_linestatus"
#   [sort]="l_suppkey,l_commitdate"
#   [target_mb]="128"
#   [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq3 --spec-glob spec_tpch_*.yaml"
#   # [start_after]="spec_tpch_RQ3_Q3.yaml"
#   [skip_load]=0
#   [skip_query]=0
# )

declare -A SCENARIO_RQ3_DEFAULT_1=(
  [name]="RQ3_DEFAULT"
  [scales]="64"
  [layouts]="no_layout,linear"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_receiptdate"
  [target_mb]="128"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq3 --spec-glob spec_tpch_*.yaml"
  # [start_after]="spec_tpch_RQ3_Q3.yaml"
  [skip_load]=0
  [skip_query]=0
)

declare -A SCENARIO_RQ3_DEFAULT_2=(
  [name]="RQ3_DEFAULT"
  [scales]="64"
  [layouts]="zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_receiptdate"
  [target_mb]="128"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq3 --spec-glob spec_tpch_*.yaml"
  # [start_after]="spec_tpch_RQ3_Q3.yaml"
  [skip_load]=0
  [skip_query]=0
)

declare -A SCENARIO_RQ3_O2_V1_1=(
  [name]="RQ3_O2_V1"
  [scales]="64"
  [layouts]="no_layout,linear"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_commitdate,l_suppkey"
  [target_mb]="128"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq3 --spec-glob spec_tpch_*.yaml"
  # [start_after]="spec_tpch_RQ3_Q3.yaml"
  [skip_load]=0
  [skip_query]=0
)

declare -A SCENARIO_RQ3_O2_V1_2=(
  [name]="RQ3_O2_V1"
  [scales]="64"
  [layouts]="zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_commitdate,l_suppkey"
  [target_mb]="128"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq3 --spec-glob spec_tpch_*.yaml"
  [start_after]="spec_tpch_RQ3_Q2_N2_4_S2_C1_N2_O1.yaml"
  [skip_load]=0
  [skip_query]=0
)

declare -A SCENARIO_RQ3_O2_V2_1=(
  [name]="RQ3_O2_V2"
  [scales]="64"
  [layouts]="no_layout,linear"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_suppkey,l_commitdate"
  [target_mb]="128"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq3 --spec-glob spec_tpch_*.yaml"
  # [start_after]="spec_tpch_RQ3_Q3.yaml"
  [skip_load]=0
  [skip_query]=0
)

declare -A SCENARIO_RQ3_O2_V2_2=(
  [name]="RQ3_O2_V2"
  [scales]="64"
  [layouts]="zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_suppkey,l_commitdate"
  [target_mb]="128"
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq3 --spec-glob spec_tpch_*.yaml"
  # [start_after]="spec_tpch_RQ3_Q3.yaml"
  [skip_load]=0
  [skip_query]=0
)

SCENARIOS=(SCENARIO_RQ3_DEFAULT_1 SCENARIO_RQ3_DEFAULT_2 SCENARIO_RQ3_O2_V1_1 SCENARIO_RQ3_O2_V1_2 SCENARIO_RQ3_O2_V2_1 SCENARIO_RQ3_O2_V2_2)
# SCENARIOS=(SCENARIO_RQ3_DEFAULT SCENARIO_RQ3_O2_V1 SCENARIO_RQ3_O2_V2)

# ---------------------------------------------------------------------------

for scenario_var in "${SCENARIOS[@]}"; do
  declare -n scenario="$scenario_var"
  name="${scenario[name]}"
  [[ -n "$name" ]] || name="scenario"
  echo "===== Running RQ3 scenario: $name ====="

  # Normalize scales list
  scales_raw="${scenario[scales]:-}"
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
    output_root="${ROOT_DIR}/workloads/tpch_rq3/${name}/scale_${scale}"

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

    # Loader overrides
    [[ -n ${scenario[record_key]:-} ]] && cmd+=(--record-key "${scenario[record_key]}")
    [[ -n ${scenario[precombine]:-} ]] && cmd+=(--precombine-field "${scenario[precombine]}")
    [[ -n ${scenario[partition]:-} ]] && cmd+=(--partition-field "${scenario[partition]}")
    [[ -n ${scenario[sort]:-} ]] && cmd+=(--sort-columns "${scenario[sort]}")
    [[ -n ${scenario[target_mb]:-} ]] && cmd+=(--target-file-mb "${scenario[target_mb]}")

    if [[ ${scenario[skip_load]:-0} -eq 1 ]]; then
      cmd+=(--skip-load)
    fi
    if [[ ${scenario[skip_query]:-0} -eq 1 ]]; then
      cmd+=(--skip-query)
    fi

    # Query args (append stats per scale; consume start_after only once)
    query_args_str="${scenario[query_args]:---workload-type custom --spec-dir workload_spec/tpch_rq3 --spec-glob spec_tpch_*.yaml}"
    query_args_str+=" --stats-file ${stats_file}"
    if [[ -n ${scenario[start_after]:-} ]]; then
      query_args_str+=" --start-after ${scenario[start_after]}"
      # avoid applying start_after to subsequent scales
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

echo "[DONE] All RQ3 scenarios completed."
