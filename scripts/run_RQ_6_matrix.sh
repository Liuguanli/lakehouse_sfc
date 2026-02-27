#!/usr/bin/env bash
set -euo pipefail

# RQ6 (TPCH-all, Hudi only): build Hudi layouts for selected tables and run the
# 10 TPCH streams under workloads/rq6_tpch_all. Layout/table configs live here.

[[ -f "${HOME}/.lakehouse/env" ]] && source "${HOME}/.lakehouse/env"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOAD_SCRIPT="${ROOT_DIR}/scripts/tpch_all/load_data.sh"
RUN_QUERIES="${ROOT_DIR}/scripts/tpch_all/run_queries.sh"
[[ -x "$LOAD_SCRIPT" && -x "$RUN_QUERIES" ]] || { echo "Missing tpch_all scripts." >&2; exit 1; }

STREAMS_ROOT="${ROOT_DIR}/workloads/rq6_tpch_all"
DATA_ROOT="${ROOT_DIR}/data/tpch_all"
RESULTS_ROOT="${ROOT_DIR}/results/tpch_rq6_hudi"
CFG_DEBUG_ROOT="${RESULTS_ROOT}/_debug_hudi_table_configs"
STREAMS_LIST="${RQ6_STREAMS:-auto}"
ACTION="count"
TAG=""
DRY_RUN="${RQ6_DRY_RUN:-0}"
SCENARIO_FILTER="${RQ6_SCENARIOS:-}"
LAYOUTS_OVERRIDE="${RQ6_LAYOUTS:-}"

# Helper: convert "a,b,c" -> "\"a\",\"b\",\"c\""
join_json_array() {
  local csv="$1"
  local first=1
  local out=""
  IFS=',' read -ra parts <<< "$csv"
  for p in "${parts[@]}"; do
    p="${p//\"/\\\"}"   # escape double quotes if any
    if [[ $first -eq 0 ]]; then
      out+=","
    fi
    first=0
    out+="\"${p}\""
  done
  printf '%s' "$out"
}

# Scenario definitions (each may include multiple tables; per-table configs live here)

# no_layout,linear,zorder,hilbert
declare -A SCENARIO_L1_O1=(
  [name]="lineitem_orders_L1_O1"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [lineitem_record_key]="l_orderkey,l_linenumber"
  [lineitem_precombine]="l_commitdate"
  [lineitem_partition]="l_returnflag,l_linestatus"
  [lineitem_sort]="l_shipdate,l_receiptdate"
  [orders_record_key]="o_orderkey"
  [orders_precombine]="o_orderdate"
  [orders_partition]="o_orderstatus,o_orderpriority"
  [orders_sort]="o_orderdate,o_orderstatus"
)

declare -A SCENARIO_L1_O2=(
  [name]="lineitem_orders_L1_O2"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [lineitem_record_key]="l_orderkey,l_linenumber"
  [lineitem_precombine]="l_commitdate"
  [lineitem_partition]="l_returnflag,l_linestatus"
  [lineitem_sort]="l_shipdate,l_receiptdate"
  [orders_record_key]="o_orderkey"
  [orders_precombine]="o_orderdate"
  [orders_partition]="o_orderstatus,o_orderpriority"
  [orders_sort]="o_orderdate,o_orderpriority"
)

declare -A SCENARIO_L1_O3=(
  [name]="lineitem_orders_L1_O3"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [lineitem_record_key]="l_orderkey,l_linenumber"
  [lineitem_precombine]="l_commitdate"
  [lineitem_partition]="l_returnflag,l_linestatus"
  [lineitem_sort]="l_shipdate,l_receiptdate"
  [orders_record_key]="o_orderkey"
  [orders_precombine]="o_orderdate"
  [orders_partition]="o_orderstatus,o_orderpriority"
  [orders_sort]="o_custkey,o_orderdate"
)

declare -A SCENARIO_L2_O1=(
  [name]="lineitem_orders_L2_O1"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [lineitem_record_key]="l_orderkey,l_linenumber"
  [lineitem_precombine]="l_commitdate"
  [lineitem_partition]="l_returnflag,l_linestatus"
  [lineitem_sort]="l_orderkey,l_suppkey"
  [orders_record_key]="o_orderkey"
  [orders_precombine]="o_orderdate"
  [orders_partition]="o_orderstatus,o_orderpriority"
  [orders_sort]="o_orderdate,o_orderstatus"
)

declare -A SCENARIO_L2_O2=(
  [name]="lineitem_orders_L2_O2"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [lineitem_record_key]="l_orderkey,l_linenumber"
  [lineitem_precombine]="l_commitdate"
  [lineitem_partition]="l_returnflag,l_linestatus"
  [lineitem_sort]="l_orderkey,l_suppkey"
  [orders_record_key]="o_orderkey"
  [orders_precombine]="o_orderdate"
  [orders_partition]="o_orderstatus,o_orderpriority"
  [orders_sort]="o_orderdate,o_orderpriority"
)

declare -A SCENARIO_L2_O3=(
  [name]="lineitem_orders_L2_O3"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [lineitem_record_key]="l_orderkey,l_linenumber"
  [lineitem_precombine]="l_commitdate"
  [lineitem_partition]="l_returnflag,l_linestatus"
  [lineitem_sort]="l_orderkey,l_suppkey"
  [orders_record_key]="o_orderkey"
  [orders_precombine]="o_orderdate"
  [orders_partition]="o_orderstatus,o_orderpriority"
  [orders_sort]="o_custkey,o_orderdate"
)

declare -A SCENARIO_L3_O1=(
  [name]="lineitem_orders_L3_O1"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [lineitem_record_key]="l_orderkey,l_linenumber"
  [lineitem_precombine]="l_commitdate"
  [lineitem_partition]="l_returnflag,l_linestatus"
  [lineitem_sort]="l_quantity,l_extendedprice"
  [orders_record_key]="o_orderkey"
  [orders_precombine]="o_orderdate"
  [orders_partition]="o_orderstatus,o_orderpriority"
  [orders_sort]="o_orderdate,o_orderstatus"
)

declare -A SCENARIO_L3_O2=(
  [name]="lineitem_orders_L3_O2"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [lineitem_record_key]="l_orderkey,l_linenumber"
  [lineitem_precombine]="l_commitdate"
  [lineitem_partition]="l_returnflag,l_linestatus"
  [lineitem_sort]="l_quantity,l_extendedprice"
  [orders_record_key]="o_orderkey"
  [orders_precombine]="o_orderdate"
  [orders_partition]="o_orderstatus,o_orderpriority"
  [orders_sort]="o_orderdate,o_orderpriority"
)

declare -A SCENARIO_L3_O3=(
  [name]="lineitem_orders_L3_O3"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [lineitem_record_key]="l_orderkey,l_linenumber"
  [lineitem_precombine]="l_commitdate"
  [lineitem_partition]="l_returnflag,l_linestatus"
  [lineitem_sort]="l_quantity,l_extendedprice"
  [orders_record_key]="o_orderkey"
  [orders_precombine]="o_orderdate"
  [orders_partition]="o_orderstatus,o_orderpriority"
  [orders_sort]="o_custkey,o_orderdate"
)

declare -A SCENARIO_L4_O1=(
  [name]="lineitem_orders_L4_O1"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [lineitem_record_key]="l_orderkey,l_linenumber"
  [lineitem_precombine]="l_commitdate"
  [lineitem_partition]="l_returnflag,l_linestatus"
  [lineitem_sort]="l_shipdate,l_receiptdate"
)

declare -A SCENARIO_L4_O2=(
  [name]="lineitem_orders_L4_O2"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [lineitem_record_key]="l_orderkey,l_linenumber"
  [lineitem_precombine]="l_commitdate"
  [lineitem_partition]="l_returnflag,l_linestatus"
  [lineitem_sort]="l_orderkey,l_suppkey"
)

declare -A SCENARIO_L4_O3=(
  [name]="lineitem_orders_L4_O3"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [lineitem_record_key]="l_orderkey,l_linenumber"
  [lineitem_precombine]="l_commitdate"
  [lineitem_partition]="l_returnflag,l_linestatus"
  [lineitem_sort]="l_quantity,l_extendedprice"
)


declare -A SCENARIO_L5_O1=(
 [name]="lineitem_orders_L5_O1"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [orders_record_key]="o_orderkey"
  [orders_precombine]="o_orderdate"
  [orders_partition]="o_orderstatus,o_orderpriority"
  [orders_sort]="o_orderdate,o_orderpriority"
)

declare -A SCENARIO_L5_O2=(
 [name]="lineitem_orders_L5_O2"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [orders_record_key]="o_orderkey"
  [orders_precombine]="o_orderdate"
  [orders_partition]="o_orderstatus,o_orderpriority"
  [orders_sort]="o_custkey,o_orderdate"
)

declare -A SCENARIO_L5_O3=(
 [name]="lineitem_orders_L5_O3"
  [tables]="customer orders lineitem supplier nation region part partsupp"
  [layouts]="linear,zorder,hilbert"
  [orders_record_key]="o_orderkey"
  [orders_precombine]="o_orderdate"
  [orders_partition]="o_orderstatus,o_orderpriority"
  [orders_sort]="o_orderdate,o_orderstatus"
)

# SCENARIOS=(SCENARIO_L1_O1 SCENARIO_L1_O2)
# SCENARIOS=(SCENARIO_L1_O1 SCENARIO_L1_O2 SCENARIO_L1_O3 SCENARIO_L2_O1 SCENARIO_L2_O2 SCENARIO_L2_O3 SCENARIO_L3_O1 SCENARIO_L3_O2 SCENARIO_L3_O3)
SCENARIOS=(SCENARIO_L4_O1 SCENARIO_L4_O2 SCENARIO_L4_O3 SCENARIO_L5_O1 SCENARIO_L5_O2 SCENARIO_L5_O3)

if [[ -n "$SCENARIO_FILTER" ]]; then
  SCENARIOS=()
  for s in ${SCENARIO_FILTER//,/ }; do
    s="${s// /}"
    [[ -z "$s" ]] && continue
    SCENARIOS+=("$s")
  done
fi

tag_arg=()
if [[ -n "$TAG" ]]; then
  TS="$(date +%Y%m%d_%H%M%S)__${TAG// /_}"
  tag_arg=(--timestamp "$TS")
fi
CFG_DEBUG_RUN_ID="$(date +%Y%m%d_%H%M%S)"
if [[ -n "$TAG" ]]; then
  CFG_DEBUG_RUN_ID="${CFG_DEBUG_RUN_ID}__${TAG// /_}"
fi
CFG_DEBUG_RUN_DIR="${CFG_DEBUG_ROOT}/${CFG_DEBUG_RUN_ID}"
mkdir -p "$CFG_DEBUG_RUN_DIR"

echo "Streams root : $STREAMS_ROOT"
echo "Data root    : $DATA_ROOT"
echo "Results root : $RESULTS_ROOT"
echo "Config debug : $CFG_DEBUG_RUN_DIR"
echo "Streams      : $STREAMS_LIST"
echo "Action       : $ACTION"
echo "Dry run      : $DRY_RUN"
[[ -n "$SCENARIO_FILTER" ]] && echo "Scenario sel : $SCENARIO_FILTER"
[[ -n "$LAYOUTS_OVERRIDE" ]] && echo "Layouts ovrd : $LAYOUTS_OVERRIDE"
[[ -n "$TAG" ]] && echo "Tag          : $TAG"

for scenario_var in "${SCENARIOS[@]}"; do
  declare -n scenario="$scenario_var"
  name="${scenario[name]}"
  layouts="${scenario[layouts]}"
  if [[ -n "$LAYOUTS_OVERRIDE" ]]; then
    layouts="$LAYOUTS_OVERRIDE"
  fi
  tables_raw="${scenario[tables]}"

  echo "===== Running scenario: ${name} (tables=${tables_raw}, layouts=${layouts}) ====="

  # Build per-table config JSON for tables in this scenario
  cfg_file="$(mktemp)"
  {
    echo "{"
    first=1
    for tbl in $tables_raw; do
      rk_key="${tbl}_record_key"
      pc_key="${tbl}_precombine"
      pt_key="${tbl}_partition"
      st_key="${tbl}_sort"
      rk="${scenario[$rk_key]:-}"
      pc="${scenario[$pc_key]:-}"
      pt="${scenario[$pt_key]:-}"
      st="${scenario[$st_key]:-}"

      # Only write overrides for tables that have explicit configs (lineitem / orders).
      if [[ -z "$rk" || -z "$pc" ]]; then
        echo "[WARN] No override for table=${tbl}, using TPCH defaults." >&2
        continue
      fi

      rk_json="$(join_json_array "$rk")"
      if [[ -n "$st" ]]; then
        st_json="$(join_json_array "$st")"
      else
        st_json=""
      fi

      [[ $first -eq 0 ]] && echo ","
      first=0

      echo -n "  \"${tbl}\": {\"record_key\": [${rk_json}], "
      echo -n "\"precombine_field\": \"${pc}\", "
      echo -n "\"partition_field\": \"${pt}\", "
      if [[ -n "$st_json" ]]; then
        echo -n "\"sort_columns\": [${st_json}]}"
      else
        echo -n "\"sort_columns\": []}"
      fi
    done
    echo
    echo "}"
  } > "$cfg_file"

  cfg_debug_path="${CFG_DEBUG_RUN_DIR}/${name}.json"
  cp "$cfg_file" "$cfg_debug_path"
  echo "[DEBUG] Saved Hudi table config JSON: $cfg_debug_path"

  for layout in ${layouts//,/ }; do
    layout_trim="${layout// /}"
    [[ -z "$layout_trim" ]] && continue

    # Load one layout at a time to keep memory usage bounded
    if [[ "$DRY_RUN" == "1" ]]; then
      echo "[DRYRUN] bash $LOAD_SCRIPT --engines hudi --tables \"$tables_raw\" --data-root \"$DATA_ROOT\" --hudi-layouts \"$layout_trim\" --hudi-table-config \"$cfg_file\" --overwrite"
    else
      bash "$LOAD_SCRIPT" \
        --engines hudi \
        --tables "$tables_raw" \
        --data-root "$DATA_ROOT" \
        --hudi-layouts "$layout_trim" \
        --hudi-table-config "$cfg_file" \
        --overwrite
    fi

    out_root="${RESULTS_ROOT}/${name}/${layout_trim}"
    if [[ "$DRY_RUN" == "1" ]]; then
      echo "[DRYRUN] bash $RUN_QUERIES --engines hudi --streams \"$STREAMS_LIST\" --streams-root \"$STREAMS_ROOT\" --data-root \"$DATA_ROOT\" --hudi-layout \"$layout_trim\" --results-root \"$out_root\" --action \"$ACTION\" ${tag_arg[*]:-}"
    else
      bash "$RUN_QUERIES" \
        --engines hudi \
        --streams "$STREAMS_LIST" \
        --streams-root "$STREAMS_ROOT" \
        --data-root "$DATA_ROOT" \
        --hudi-layout "$layout_trim" \
        --results-root "$out_root" \
        --action "$ACTION" \
        "${tag_arg[@]}"
    fi
  done

  rm -f "$cfg_file"
done

echo "[DONE] RQ6 Hudi streams completed."
