#!/usr/bin/env bash
set -euo pipefail

SPEC_DIR=workload_spec/tpch_rq1
OUT_ROOT=workloads/tpch_rq1/SCENARIO_DEFAULT
STATS=workloads/stats/tpch_16_stats.yaml
TABLE=lineitem

mkdir -p "$OUT_ROOT/yaml" "$OUT_ROOT/sql"

for spec in "$SPEC_DIR"/spec_tpch_RQ1_*.yaml; do
  name=$(basename "$spec" .yaml)
  yaml_out="$OUT_ROOT/yaml/${name}.yaml"
  sql_out_dir="$OUT_ROOT/sql/${name}"
  mkdir -p "$sql_out_dir"
  python -m wlg.cli fill \
    --spec "$spec" \
    --stats "$STATS" \
    --out "$yaml_out" \
    --sql-dir "$sql_out_dir"
done

# Amazon RQ1
SPEC_DIR_AMZ=workload_spec/amazon_rq1
OUT_ROOT_AMZ=workloads/amazon_rq1/SCENARIO_DEFAULT
STATS_AMZ=workloads/stats/amazon_stats.yaml
TABLE_AMZ=amazon

mkdir -p "$OUT_ROOT_AMZ/yaml" "$OUT_ROOT_AMZ/sql"

for spec in "$SPEC_DIR_AMZ"/spec_amazon_RQ1_*.yaml; do
  name=$(basename "$spec" .yaml)
  yaml_out="$OUT_ROOT_AMZ/yaml/${name}.yaml"
  sql_out_dir="$OUT_ROOT_AMZ/sql/${name}"
  mkdir -p "$sql_out_dir"
  python -m wlg.cli fill \
    --spec "$spec" \
    --stats "$STATS_AMZ" \
    --out "$yaml_out" \
    --sql-dir "$sql_out_dir"
done
