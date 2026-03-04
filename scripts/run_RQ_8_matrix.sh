#!/usr/bin/env bash
set -euo pipefail

# RQ8 matrix runner:
# 1) optionally generate RQ8 specs (S1 by default)
# 2) run a sort-key matrix via scripts/run_RQ_1.sh using custom RQ8 specs
#
# Defaults are env-overridable:
#   RQ8_SELECTIVITY=S1
#   RQ8_LAYOUTS=no_layout,linear,zorder,hilbert
#   RQ8_SCALES=16
#   RQ8_GENERATE_SPECS=1
#   RQ8_FILL_SPECS=0
#   RQ8_FILL_FORCE=0
#   RQ8_SKIP_LOAD=0
#   RQ8_SKIP_QUERY=0
#   RQ8_SCENARIOS=SCENARIO_L1,SCENARIO_L2,...
#   RQ8_DRY_RUN=0

[[ -f "${HOME}/.lakehouse/env" ]] && source "${HOME}/.lakehouse/env"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_RQ1="${ROOT_DIR}/scripts/run_RQ_1.sh"
GEN_RQ8="${ROOT_DIR}/workload_spec/generate_tpch_rq8_specs.py"

[[ -x "$RUN_RQ1" ]] || { echo "Missing runner: $RUN_RQ1" >&2; exit 1; }
[[ -f "$GEN_RQ8" ]] || { echo "Missing generator: $GEN_RQ8" >&2; exit 1; }

RQ8_SELECTIVITY="${RQ8_SELECTIVITY:-S1}"
RQ8_LAYOUTS="${RQ8_LAYOUTS:-no_layout,linear,zorder,hilbert}"
RQ8_SCALES="${RQ8_SCALES:-16}"
# Keep file-size setting aligned with RQ1 matrix defaults.
RQ1_TARGET_MB_FIXED="128"
RQ8_SPEC_DIR="${RQ8_SPEC_DIR:-${ROOT_DIR}/workload_spec/tpch_rq8}"
RQ8_SPEC_GLOB="${RQ8_SPEC_GLOB:-spec_tpch_RQ8_*_${RQ8_SELECTIVITY}_*.yaml}"
RQ8_GENERATE_SPECS="${RQ8_GENERATE_SPECS:-1}"
RQ8_FILL_SPECS="${RQ8_FILL_SPECS:-0}"
RQ8_FILL_FORCE="${RQ8_FILL_FORCE:-0}"
RQ8_SKIP_LOAD="${RQ8_SKIP_LOAD:-0}"
RQ8_SKIP_QUERY="${RQ8_SKIP_QUERY:-0}"
RQ8_SCENARIOS="${RQ8_SCENARIOS:-}"
RQ8_DRY_RUN="${RQ8_DRY_RUN:-0}"
RQ8_QUERY_ARGS_OVERRIDE="${RQ8_QUERY_ARGS_OVERRIDE:-}"

declare -A SCENARIO_L1=(
  [name]="RQ8_L1_ship_receipt"
  [sort]="l_shipdate,l_receiptdate"
)
declare -A SCENARIO_L2=(
  [name]="RQ8_L2_commit_supp"
  [sort]="l_commitdate,l_suppkey"
)
declare -A SCENARIO_L3=(
  [name]="RQ8_L3_order_supp"
  [sort]="l_orderkey,l_suppkey"
)
declare -A SCENARIO_L4=(
  [name]="RQ8_L4_price_qty"
  [sort]="l_extendedprice,l_quantity"
)

SCENARIOS=(SCENARIO_L1 SCENARIO_L2 SCENARIO_L3 SCENARIO_L4)
# SCENARIOS=(SCENARIO_L3 SCENARIO_L4)

if [[ -n "$RQ8_SCENARIOS" ]]; then
  SCENARIOS=()
  for s in ${RQ8_SCENARIOS//,/ }; do
    s="${s// /}"
    [[ -z "$s" ]] && continue
    SCENARIOS+=("$s")
  done
fi

if [[ "$RQ8_GENERATE_SPECS" == "1" ]]; then
  gen_cmd=(python "$GEN_RQ8" --selectivity "$RQ8_SELECTIVITY")
  if [[ "$RQ8_FILL_SPECS" == "1" ]]; then
    gen_cmd+=(--fill)
    [[ "$RQ8_FILL_FORCE" == "1" ]] && gen_cmd+=(--fill-force)
  fi
  echo "[STEP] Generate RQ8 specs: ${gen_cmd[*]}"
  if [[ "$RQ8_DRY_RUN" == "1" ]]; then
    echo "[DRY-RUN] skip generator execution"
  else
    "${gen_cmd[@]}"
  fi
fi

query_args_str="--workload-type custom --spec-dir ${RQ8_SPEC_DIR} --spec-glob ${RQ8_SPEC_GLOB}"
if [[ -n "$RQ8_QUERY_ARGS_OVERRIDE" ]]; then
  query_args_str="$RQ8_QUERY_ARGS_OVERRIDE"
fi

echo "===== RQ8 matrix ====="
echo "Selectivity : $RQ8_SELECTIVITY"
echo "Spec dir    : $RQ8_SPEC_DIR"
echo "Spec glob   : $RQ8_SPEC_GLOB"
echo "Layouts     : $RQ8_LAYOUTS"
echo "Scales      : $RQ8_SCALES"
echo "Target MB   : $RQ1_TARGET_MB_FIXED (same as RQ1 matrix)"
echo "Scenarios   : ${SCENARIOS[*]}"
echo "Query args  : $query_args_str"
echo "Skip load   : $RQ8_SKIP_LOAD"
echo "Skip query  : $RQ8_SKIP_QUERY"
echo "Dry-run     : $RQ8_DRY_RUN"

for scenario_var in "${SCENARIOS[@]}"; do
  declare -n scenario="$scenario_var"
  name="${scenario[name]}"
  sort_cols="${scenario[sort]}"

  output_root="${ROOT_DIR}/workloads/tpch_rq8/${name}"
  cmd=(
    "$RUN_RQ1"
    --dataset tpch
    --dataset-name tpch_16
    --scales "$RQ8_SCALES"
    --layouts "$RQ8_LAYOUTS"
    --record-key "l_orderkey,l_linenumber"
    --precombine-field "l_commitdate"
    --partition-field "l_returnflag,l_linestatus"
    --sort-columns "$sort_cols"
    --target-file-mb "$RQ1_TARGET_MB_FIXED"
    --output-root "$output_root"
    --tag "rq=RQ8"
    --tag "scenario=${name}"
  )

  [[ "$RQ8_SKIP_LOAD" == "1" ]] && cmd+=(--skip-load)
  [[ "$RQ8_SKIP_QUERY" == "1" ]] && cmd+=(--skip-query)

  read -r -a qargs <<<"$query_args_str"
  cmd+=(-- "${qargs[@]}")

  echo "----- Scenario: ${name} (sort=${sort_cols}) -----"
  echo "${cmd[*]}"
  if [[ "$RQ8_DRY_RUN" == "1" ]]; then
    continue
  fi
  RQ_MATRIX_SCENARIO="$name" bash "${cmd[@]}"
done

echo "[DONE] RQ8 matrix completed."
