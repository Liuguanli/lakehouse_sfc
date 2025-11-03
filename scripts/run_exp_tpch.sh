
source ~/.lakehouse/env

## Generate profile
STATS_FILE="tpch_16_stats.yaml"
# python -m wlg.cli profile --input /datasets/tpch_16.parquet --format parquet --out "$STATS_FILE" --sample-rows 200000 --seed 42 --infer_dates

# Generate workloads
for q_idx in $(seq 1 7); do
  spec_path="workload_spec/spec_tpch_16_Q${q_idx}.yaml"
  sql_dir="workloads/tpch_16_Q${q_idx}"
  out_path="workloads/yaml/tpch_16_q${q_idx}.yaml"

  if [[ ! -f "$spec_path" ]]; then
    echo "Spec not found for Q${q_idx}: ${spec_path}, skipping." >&2
    continue
  fi
  if [[ ! -d "$sql_dir" ]]; then
    echo "SQL directory not found for Q${q_idx}: ${sql_dir}, skipping." >&2
    continue
  fi

  python -m wlg.cli fill \
    --spec "$spec_path" \
    --stats "$STATS_FILE" \
    --out "$out_path" \
    --sql-dir "$sql_dir"

  bash ./scripts/run_query.sh "$sql_dir"
done