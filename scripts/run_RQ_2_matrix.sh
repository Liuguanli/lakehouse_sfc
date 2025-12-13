#!/usr/bin/env bash
set -euo pipefail

# bash ./scripts/lakehouse_setup.sh --repair

source ~/.lakehouse/env

#
# Scenario runner for RQ2. Define multiple loader/query configurations and let
# this script invoke scripts/run_RQ_1.sh for each scenario sequentially.
#
# Customize the scenario associative arrays below as needed.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_SCRIPT="${ROOT_DIR}/scripts/run_RQ_1.sh"
START_AFTER_SPEC="${RQ2_START_AFTER:-}"
START_AFTER_CONSUMED=0
DEFAULT_RQ2_QUERY_ARGS="--spec-dir workload_spec/tpch_rq2 --spec-glob spec_tpch_RQ2_*_C1_N3_O1.yaml --workload-type custom --output-root workloads/tpch_rq2"

[[ -x "$RUN_SCRIPT" ]] || { echo "Missing run script: $RUN_SCRIPT" >&2; exit 1; }

DEFAULT_SCALES="16"

clean_spark_eventlogs() {
  # Derive the on-disk path Spark writes event logs to (strip file:// if present).
  local ev_dir="${SPARK_EVENTLOG_DIR:-${ROOT_DIR}/spark_eventlogs}"
  ev_dir="${ev_dir#file://}"
  if [[ -d "$ev_dir" ]]; then
    echo "[CLEAN] Removing Spark event logs under $ev_dir"
    # Best-effort cleanup; don't fail the matrix run if a file can't be removed.
    find "$ev_dir" -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true
  else
    echo "[SKIP CLEAN] Spark eventlog dir not found: $ev_dir"
  fi
}

# ---------------------------------------------------------------------------
# Scenario definitions (edit to taste)
# Each associative array may set the following keys:
#   name             : label for logging/output directories
#   scales           : space/comma separated list of scales (default 16)
#   layouts          : layout list (default no_layout,linear,zorder,hilbert)
#   record_key       : RECORD_KEY override for loader
#   precombine       : PRECOMBINE_FIELD override
#   partition        : PARTITION_FIELD override
#   sort             : SORT_COLUMNS override
#   target_mb        : TARGET_FILE_MB override
#   input_template   : HUDI_INPUT_TEMPLATE override
#   base_template    : HUDI_BASE_TEMPLATE override
#   output_root      : explicit query output root (defaults to workloads/tpch_rq2/<name>)
#   query_args       : extra arguments passed to run_RQ_query.sh (string)
#   skip_load        : set to 1 to skip load stage (use existing data)
#   skip_query       : set to 1 to skip query stage
#
# Provide additional scenarios by copying the template below.

# COLUMN_CONFIGS = {
#     "C1_N2_O1": ["l_shipdate", "l_receiptdate"],
#     "C1_N2_O2": ["l_receiptdate", "l_shipdate"],
#     "C2_N2_O1": ["l_commitdate", "l_suppkey"],
#     "C2_N2_O2": ["l_suppkey", "l_commitdate"],
#     "C3_N2_O1": ["l_orderkey", "l_suppkey"],
#     "C3_N2_O2": ["l_suppkey", "l_orderkey"],
#     "C4_N2_O1": ["l_extendedprice", "l_quantity"],
#     "C4_N2_O2": ["l_quantity", "l_extendedprice"],
# }

declare -A RQ2_SCENARIO_DEFAULT=(
  [name]="RQ2_SCENARIO_DEFAULT"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_receiptdate,l_commitdate"
  [target_mb]="128"
  # [query_args]="--limit 5"
)

declare -A RQ2_SCENARIO_DEFAULT_V1=(
  [name]="RQ2_SCENARIO_DEFAULT_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_commitdate,l_receiptdate"
  [target_mb]="128"
  # [query_args]="--limit 5"
)

declare -A RQ2_SCENARIO_DEFAULT_V2=(
  [name]="RQ2_SCENARIO_DEFAULT_V2"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_commitdate,l_shipdate,l_receiptdate"
  [target_mb]="128"
  # [query_args]="--limit 5"
)

declare -A RQ2_SCENARIO_DEFAULT_V3=(
  [name]="RQ2_SCENARIO_DEFAULT_V3"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_receiptdate,l_shipdate,l_commitdate"
  [target_mb]="128"
)

declare -A RQ2_SCENARIO_DEFAULT_V4=(
  [name]="RQ2_SCENARIO_DEFAULT_V4"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_receiptdate,l_commitdate,l_shipdate"
  [target_mb]="128"
)

declare -A RQ2_SCENARIO_DEFAULT_V5=(
  [name]="RQ2_SCENARIO_DEFAULT_V5"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_commitdate,l_receiptdate,l_shipdate"
  [target_mb]="128"
)

declare -A RQ2_SCENARIO_O2_V1=(
  [name]="RQ2_SCENARIO_O2_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_extendedprice,l_quantity,l_shipdate"
  [target_mb]="128"
)

declare -A RQ2_SCENARIO_O2_V2=(
  [name]="RQ2_SCENARIO_O2_V2"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_quantity,l_extendedprice,l_shipdate"
  [target_mb]="128"
)

declare -A RQ2_SCENARIO_O2_V3=(
  [name]="RQ2_SCENARIO_O2_V3"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_extendedprice,l_shipdate,l_quantity"
  [target_mb]="128"
)


declare -A RQ2_SCENARIO_O2_V4=(
  [name]="RQ2_SCENARIO_O2_V4"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_quantity,l_shipdate,l_extendedprice"
  [target_mb]="128"
)

declare -A RQ2_SCENARIO_O2_V5=(
  [name]="RQ2_SCENARIO_O2_V5"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_extendedprice,l_quantity"
  [target_mb]="128"
)

declare -A RQ2_SCENARIO_O2_V6=(
  [name]="RQ2_SCENARIO_O2_V6"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_quantity,l_extendedprice"
  [target_mb]="128"
)

declare -A RQ2_SCENARIO_O3_V1=(
  [name]="RQ2_SCENARIO_O3_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_suppkey,l_shipdate,l_extendedprice,l_quantity"
  [target_mb]="128"
)

declare -A RQ2_SCENARIO_O3_V2=(
  [name]="RQ2_SCENARIO_O3_V2"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_suppkey,l_extendedprice,l_quantity"
  [target_mb]="128"
)


declare -A RQ2_SCENARIO_O4_V1=(
  [name]="RQ2_SCENARIO_O4_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_commitdate,l_suppkey,l_extendedprice,l_quantity"
  [target_mb]="128"
)

# declare -A SCENARIO_ALT_SORT=(
#   [name]="shipdate_only"
#   [scales]="16"
#   [layouts]="no_layout,hilbert"
#   [record_key]="l_orderkey,l_linenumber"
#   [sort]="l_shipdate"
#   [query_args]="--spec spec_tpch_RQ2_Q1_S1_C1_N2_O1.yaml --skip-run"
# )

# SCENARIOS=(RQ2_SCENARIO_DEFAULT_V5 RQ2_SCENARIO_O2_V1 RQ2_SCENARIO_O2_V2 RQ2_SCENARIO_O2_V3 RQ2_SCENARIO_O2_V4 RQ2_SCENARIO_O2_V5 RQ2_SCENARIO_O2_V6 RQ2_SCENARIO_O3_V1 RQ2_SCENARIO_O3_V2 RQ2_SCENARIO_O4_V1)
SCENARIOS=(RQ2_SCENARIO_DEFAULT RQ2_SCENARIO_DEFAULT_V1 RQ2_SCENARIO_DEFAULT_V2 RQ2_SCENARIO_DEFAULT_V3 RQ2_SCENARIO_DEFAULT_V4 RQ2_SCENARIO_DEFAULT_V5 RQ2_SCENARIO_O2_V1 RQ2_SCENARIO_O2_V2 RQ2_SCENARIO_O2_V3 RQ2_SCENARIO_O2_V4 RQ2_SCENARIO_O2_V5 RQ2_SCENARIO_O2_V6 RQ2_SCENARIO_O3_V1 RQ2_SCENARIO_O3_V2 RQ2_SCENARIO_O4_V1)

# ---------------------------------------------------------------------------

for scenario_var in "${SCENARIOS[@]}"; do
  declare -n scenario="$scenario_var"
  name="${scenario[name]}"
  [[ -n "$name" ]] || name="scenario"
  echo "===== Running scenario: $name ====="

  scenario_dataset="${scenario[dataset]:-tpch}"
  cmd=( "$RUN_SCRIPT" )
  cmd+=(--dataset "$scenario_dataset")
  [[ -n ${scenario[dataset_name]:-} ]] && cmd+=(--dataset-name "${scenario[dataset_name]}")

  [[ -n ${scenario[scales]:-} ]] && cmd+=(--scales "${scenario[scales]}")
  [[ -n ${scenario[layouts]:-} ]] && cmd+=(--layouts "${scenario[layouts]}")
  [[ -n ${scenario[record_key]:-} ]] && cmd+=(--record-key "${scenario[record_key]}")
  [[ -n ${scenario[precombine]:-} ]] && cmd+=(--precombine-field "${scenario[precombine]}")
  [[ -n ${scenario[partition]:-} ]] && cmd+=(--partition-field "${scenario[partition]}")
  [[ -n ${scenario[sort]:-} ]] && cmd+=(--sort-columns "${scenario[sort]}")
  [[ -n ${scenario[target_mb]:-} ]] && cmd+=(--target-file-mb "${scenario[target_mb]}")
  [[ -n ${scenario[input_template]:-} ]] && cmd+=(--input-template "${scenario[input_template]}")
  [[ -n ${scenario[base_template]:-} ]] && cmd+=(--base-template "${scenario[base_template]}")
  cmd+=(--tag "scenario=${name}")

  if [[ ${scenario[skip_load]:-0} -eq 1 ]]; then
    cmd+=(--skip-load)
  fi
  if [[ ${scenario[skip_query]:-0} -eq 1 ]]; then
    cmd+=(--skip-query)
  fi

  if [[ -n "$START_AFTER_SPEC" && $START_AFTER_CONSUMED -eq 0 ]]; then
    cmd+=(--start-after "$START_AFTER_SPEC")
    START_AFTER_CONSUMED=1  # only apply to the first scenario
  fi

  # Default to RQ2 spec dir/glob unless overridden per scenario.
  scenario_query_args_str="${scenario[query_args]:-$DEFAULT_RQ2_QUERY_ARGS}"
  if [[ -n "$scenario_query_args_str" ]]; then
    read -r -a scenario_query_args <<<"$scenario_query_args_str"
    cmd+=(-- "${scenario_query_args[@]}")
  fi

  if [[ -n ${scenario[query_args]:-} ]]; then
    (
      declare -f usage >/dev/null 2>&1 || true
    )
  fi
  bash "${cmd[@]}"

  if [[ ${scenario[skip_load]:-0} -eq 0 ]]; then
    if [[ "$scenario_dataset" == "amazon" ]]; then
      echo "[CLEAN] Removing generated Amazon data for scenario=${name}"
      bash "${ROOT_DIR}/scripts/clean_data.sh" --yes --amazon
    else
      scales_val="${scenario[scales]:-$DEFAULT_SCALES}"
      echo "[CLEAN] Removing generated data for scenario=${name}, scales=${scales_val}"
      bash "${ROOT_DIR}/scripts/clean_data.sh" --yes --scales "${scales_val}"
    fi
  else
    echo "[SKIP CLEAN] skip_load enabled for scenario=${name}, retaining existing data."
  fi

  clean_spark_eventlogs
done

echo "[DONE] All scenarios completed."
