#!/usr/bin/env bash
set -euo pipefail

# bash ./scripts/lakehouse_setup.sh --repair

source ~/.lakehouse/env

#
# Scenario runner for RQ1. Define multiple loader/query configurations and let
# this script invoke scripts/run_RQ_1.sh for each scenario sequentially.
#
# Customize the scenario associative arrays below as needed.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Allow overriding the runner to decouple matrix logic from RQ1 specifics.
RUN_SCRIPT="${RQ_MATRIX_RUN_SCRIPT:-${ROOT_DIR}/scripts/run_RQ_1.sh}"
[[ -x "$RUN_SCRIPT" ]] || { echo "Missing run script: $RUN_SCRIPT" >&2; exit 1; }

DEFAULT_SCALES="16"

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
#   output_root      : explicit query output root (defaults to workloads/tpch_rq1/<name>)
#   query_args       : extra arguments passed to run_RQ_query.sh (string)
#   start_after_spec : --start-after argument passed to run_RQ_query.sh
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

declare -A SCENARIO_DEFAULT=(
  [name]="SCENARIO_DEFAULT"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_receiptdate"
  [target_mb]="128"
  [start_after_spec]="spec_tpch_RQ1_Q3_K1_5_S0_C4_N2_O2.yaml"
  [skip_load]=1
  # [query_args]="--limit 5"
)

declare -A SCENARIO_DEFAULT_V1=(
  [name]="SCENARIO_DEFAULT_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_receiptdate,l_shipdate"
  [target_mb]="128"
)

declare -A SCENARIO_O2_V1=(
  [name]="SCENARIO_O2_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_commitdate,l_suppkey"
  [target_mb]="128"
)

declare -A SCENARIO_O2_V2=(
  [name]="SCENARIO_O2_V2"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_suppkey,l_commitdate"
  [target_mb]="128"
)

declare -A SCENARIO_O3_V1=(
  [name]="SCENARIO_O3_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_orderkey,l_suppkey"
  [target_mb]="128"
)

declare -A SCENARIO_O3_V2=(
  [name]="SCENARIO_O3_V2"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_suppkey,l_orderkey"
  [target_mb]="128"
)

declare -A SCENARIO_O4_V1=(
  [name]="SCENARIO_O4_V1"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_extendedprice,l_quantity"
  [target_mb]="128"
)

declare -A SCENARIO_O4_V2=(
  [name]="SCENARIO_O4_V2"
  [scales]="16"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_quantity,l_extendedprice"
  [target_mb]="128"
)


declare -A SCENARIO_AMAZON_DEFAULT=(
  [name]="amazon_default"
  [dataset]="amazon"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="user_id,asin"
  [precombine]="record_timestamp"
  [partition]="category"
  [sort]="asin,parent_asin"
  [target_mb]="128"
  [start_after_spec]="spec_amazon_RQ1_Q3_K1_1_S0_C5.yaml"
  [skip_load]=1
)

declare -A SCENARIO_AMAZON_DEFAULT_V1=(
  [name]="amazon_default"
  [dataset]="amazon"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="user_id,asin"
  [precombine]="record_timestamp"
  [partition]="category"
  [sort]="parent_asin,asin"
  [target_mb]="128"
)

declare -A SCENARIO_AMAZON_O2_V1=(
  [name]="amazon_sort_variant"
  [dataset]="amazon"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="user_id,asin"
  [precombine]="record_timestamp"
  [partition]="category"
  [sort]="asin,user_id"
  [target_mb]="128"
)

declare -A SCENARIO_AMAZON_O2_V2=(
  [name]="amazon_sort_variant"
  [dataset]="amazon"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="user_id,asin"
  [precombine]="record_timestamp"
  [partition]="category"
  [sort]="user_id,asin"
  [target_mb]="128"
)

declare -A SCENARIO_AMAZON_O3_V1=(
  [name]="amazon_sort_variant"
  [dataset]="amazon"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="user_id,asin"
  [precombine]="record_timestamp"
  [partition]="category"
  [sort]="user_id,record_timestamp"
  [target_mb]="128"
)

declare -A SCENARIO_AMAZON_O3_V2=(
  [name]="amazon_sort_variant"
  [dataset]="amazon"
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="user_id,asin"
  [precombine]="record_timestamp"
  [partition]="category"
  [sort]="record_timestamp,user_id"
  [target_mb]="128"
)

# declare -A SCENARIO_ALT_SORT=(
#   [name]="shipdate_only"
#   [scales]="16"
#   [layouts]="no_layout,hilbert"
#   [record_key]="l_orderkey,l_linenumber"
#   [sort]="l_shipdate"
#   [query_args]="--spec spec_tpch_RQ1_Q1_S1_C1_N2_O1.yaml --skip-run"
# )

SCENARIOS=(SCENARIO_DEFAULT SCENARIO_DEFAULT_V1 SCENARIO_O2_V1 SCENARIO_O2_V2 SCENARIO_O3_V1 SCENARIO_O3_V2 SCENARIO_O4_V1 SCENARIO_O4_V2)
# SCENARIOS=(SCENARIO_AMAZON_DEFAULT SCENARIO_AMAZON_DEFAULT_V1 SCENARIO_AMAZON_O2_V1 SCENARIO_AMAZON_O2_V2 SCENARIO_AMAZON_O3_V1 SCENARIO_AMAZON_O3_V2)

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

  if [[ -n ${scenario[start_after_spec]:-} ]]; then
    cmd+=(--start-after "${scenario[start_after_spec]}")
  fi

  if [[ -n ${scenario[query_args]:-} ]]; then
    read -r -a scenario_query_args <<<"${scenario[query_args]}"
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
done

echo "[DONE] All scenarios completed."
