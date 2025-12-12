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
STREAMS_LIST="auto"
ACTION="count"
TAG=""

# Scenario definitions (each may include multiple tables; per-table configs live here)

declare -A SCENARIO_L1_O1=(
  [name]="lineitem_orders_L1_O1"
  [tables]="lineitem orders"
  [layouts]="no_layout,linear,zorder,hilbert"
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
  [tables]="lineitem orders"
  [layouts]="no_layout,linear,zorder,hilbert"
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
  [tables]="lineitem orders"
  [layouts]="no_layout,linear,zorder,hilbert"
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
  [tables]="lineitem orders"
  [layouts]="no_layout,linear,zorder,hilbert"
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
  [tables]="lineitem orders"
  [layouts]="no_layout,linear,zorder,hilbert"
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
  [tables]="lineitem orders"
  [layouts]="no_layout,linear,zorder,hilbert"
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
  [tables]="lineitem orders"
  [layouts]="no_layout,linear,zorder,hilbert"
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
  [tables]="lineitem orders"
  [layouts]="no_layout,linear,zorder,hilbert"
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
  [tables]="lineitem orders"
  [layouts]="no_layout,linear,zorder,hilbert"
  [lineitem_record_key]="l_orderkey,l_linenumber"
  [lineitem_precombine]="l_commitdate"
  [lineitem_partition]="l_returnflag,l_linestatus"
  [lineitem_sort]="l_quantity,l_extendedprice"
  [orders_record_key]="o_orderkey"
  [orders_precombine]="o_orderdate"
  [orders_partition]="o_orderstatus,o_orderpriority"
  [orders_sort]="o_custkey,o_orderdate"
)

SCENARIOS=(SCENARIO_L1_O1 SCENARIO_L1_O2 SCENARIO_L1_O3 SCENARIO_L2_O1 SCENARIO_L2_O2 SCENARIO_L2_O3 SCENARIO_L3_O1 SCENARIO_L3_O2 SCENARIO_L3_O3)

tag_arg=()
if [[ -n "$TAG" ]]; then
  TS="$(date +%Y%m%d_%H%M%S)__${TAG// /_}"
  tag_arg=(--timestamp "$TS")
fi

echo "Streams root : $STREAMS_ROOT"
echo "Data root    : $DATA_ROOT"
echo "Results root : $RESULTS_ROOT"
echo "Streams      : $STREAMS_LIST"
echo "Action       : $ACTION"
[[ -n "$TAG" ]] && echo "Tag          : $TAG"

for scenario_var in "${SCENARIOS[@]}"; do
  declare -n scenario="$scenario_var"
  name="${scenario[name]}"
  layouts="${scenario[layouts]}"
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
      [[ -z "$rk" || -z "$pc" ]] && { echo "[WARN] Missing config for table=${tbl}, skipping."; continue; }
      [[ $first -eq 0 ]] && echo ","
      first=0
      echo -n "  \"${tbl}\": {\"record_key\": [\"${rk//,/\",\"}\"], "
      echo -n "\"precombine_field\": \"${pc}\", "
      echo -n "\"partition_field\": \"${pt}\", "
      echo -n "\"sort_columns\": [\"${st//,/\",\"}\"]}"
    done
    echo
    echo "}"
  } > "$cfg_file"

  bash "$LOAD_SCRIPT" \
    --engines hudi \
    --tables "$tables_raw" \
    --data-root "$DATA_ROOT" \
    --hudi-layouts "$layouts" \
    --hudi-table-config "$cfg_file" \
    --overwrite

  for layout in ${layouts//,/ }; do
    layout_trim="${layout// /}"
    [[ -z "$layout_trim" ]] && continue
    out_root="${RESULTS_ROOT}/${name}/${layout_trim}"
    bash "$RUN_QUERIES" \
      --engines hudi \
      --streams "$STREAMS_LIST" \
      --streams-root "$STREAMS_ROOT" \
      --data-root "$DATA_ROOT" \
      --hudi-layout "$layout_trim" \
      --results-root "$out_root" \
      --action "$ACTION" \
      "${tag_arg[@]}"
  done

  rm -f "$cfg_file"
done

echo "[DONE] RQ6 Hudi streams completed."
