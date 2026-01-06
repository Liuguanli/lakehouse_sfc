#!/usr/bin/env bash
set -euo pipefail

# RQ7: incremental updates – start from TPCH_16 base and incrementally insert
# shuffled TPCH_4 rows in 10% batches, running queries after each insert.

[[ -f "${HOME}/.lakehouse/env" ]] && source "${HOME}/.lakehouse/env"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_RQ1="${ROOT_DIR}/scripts/run_RQ_1.sh"
CLEAN_SCRIPT="${ROOT_DIR}/scripts/clean_data.sh"
BATCH_BUILDER="${ROOT_DIR}/lakehouse_op/build_tpch_update_batches.py"
HUDI_UPSERT_SCRIPT="${ROOT_DIR}/lakehouse_op/hudi_upsert.py"

DEFAULT_INPUT_TEMPLATE="/datasets/tpch_%s.parquet"
DEFAULT_BASE_TEMPLATE="./data/tpch_%s/hudi"
HUDI_PKG="${HUDI_PKG:-org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2}"

# ---------------------------------------------------------------------------
# Scenario definition
# ---------------------------------------------------------------------------
declare -A SCENARIO_RQ7_DEFAULT=(
  [name]="RQ7_tpch16_updates_from_4"
  [base_scale]=16
  [source_scale]=4
  [layouts]="no_layout,linear,zorder,hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_receiptdate"
  [target_mb]="128"
  [batches]=10              # 10% increments of TPCH_4
  [shuffle_seed]=42
  [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq7 --spec-glob spec_tpch_*.yaml"
)

SCENARIOS=(SCENARIO_RQ7_DEFAULT)

run_queries_after_batch() {
  local layout="$1" batch_label="$2" name="$3" base_scale="$4" stats_file="$5" query_args_str="$6"
  local output_root="${ROOT_DIR}/workloads/tpch_rq7/${name}/layout_${layout}/batch_${batch_label}"
  mkdir -p "$output_root"

  read -r -a query_args <<<"$query_args_str"
  HUDI_LAYOUTS="$layout" \
    bash "${ROOT_DIR}/scripts/run_RQ_query.sh" \
      --dataset-name "tpch_${base_scale}" \
      --output-root "$output_root" \
      --stats-file "$stats_file" \
      --tag "scenario=${name}" \
      --tag "layout=${layout}" \
      --tag "batch=${batch_label}" \
      "${query_args[@]}"
}

for scenario_var in "${SCENARIOS[@]}"; do
  declare -n scenario="$scenario_var"
  name="${scenario[name]}"
  base_scale="${scenario[base_scale]:-16}"
  src_scale="${scenario[source_scale]:-4}"
  layouts="${scenario[layouts]:-no_layout,linear,zorder,hilbert}"
  record_key="${scenario[record_key]:-l_orderkey,l_linenumber}"
  precombine="${scenario[precombine]:-l_commitdate}"
  partition="${scenario[partition]:-l_returnflag,l_linestatus}"
  sort_cols="${scenario[sort]:-l_shipdate,l_receiptdate}"
  target_mb="${scenario[target_mb]:-128}"
  batch_count="${scenario[batches]:-10}"
  seed="${scenario[shuffle_seed]:-42}"
  input_template="${scenario[input_template]:-${HUDI_INPUT_TEMPLATE:-$DEFAULT_INPUT_TEMPLATE}}"
  base_template="${scenario[base_template]:-${HUDI_BASE_TEMPLATE:-$DEFAULT_BASE_TEMPLATE}}"
  query_args_str="${scenario[query_args]:---workload-type custom --spec-dir workload_spec/tpch_rq3 --spec-glob spec_tpch_*.yaml}"

  stats_file="${ROOT_DIR}/workloads/stats/tpch_${base_scale}_stats.yaml"
  [[ -f "$stats_file" ]] || { echo "Missing stats file: $stats_file" >&2; exit 1; }

  base_dir=$(printf "$base_template" "$base_scale")
  src_input=$(printf "$input_template" "$src_scale")
  batch_root="${ROOT_DIR}/data/tmp/rq7_tpch${src_scale}_seed${seed}"

  echo "===== RQ7 scenario: ${name} ====="
  echo "Base scale : ${base_scale}"
  echo "Source data: ${src_input} (scale ${src_scale})"
  echo "Layouts    : ${layouts}"
  echo "Batches    : ${batch_count} (10% each)"

  echo "[STEP] Clean base data scale=${base_scale}"
  bash "$CLEAN_SCRIPT" --yes --scales "$base_scale"

  echo "[STEP] Build base TPCH_${base_scale} layouts"
  (
    export HUDI_BASE_TEMPLATE="$base_template"
    export HUDI_INPUT_TEMPLATE="$input_template"
    cmd=( "$RUN_RQ1"
          --scales "$base_scale"
          --layouts "$layouts"
          --dataset tpch
          --dataset-name "tpch_${base_scale}"
          --skip-query )
    [[ -n "$record_key" ]] && cmd+=(--record-key "$record_key")
    [[ -n "$precombine" ]] && cmd+=(--precombine-field "$precombine")
    [[ -n "$partition" ]] && cmd+=(--partition-field "$partition")
    [[ -n "$sort_cols" ]] && cmd+=(--sort-columns "$sort_cols")
    [[ -n "$target_mb" ]] && cmd+=(--target-file-mb "$target_mb")
    bash "${cmd[@]}"
  )

  echo "[STEP] Prepare shuffled batches from TPCH_${src_scale}"
  "$SPARK_HOME/bin/spark-submit" \
    "$BATCH_BUILDER" \
    --source "$src_input" \
    --output-dir "$batch_root" \
    --batches "$batch_count" \
    --seed "$seed"

  IFS=',' read -r -a layout_arr <<<"$layouts"
  for layout in "${layout_arr[@]}"; do
    layout_token="${layout// /}"
    [[ -z "$layout_token" ]] && continue
    table_path="${base_dir}/hudi_${layout_token}"
    table_name="events_hudi_${layout_token}"

    echo "[STEP] Baseline queries layout=${layout_token} (before inserts)"
    run_queries_after_batch "$layout_token" 0 "$name" "$base_scale" "$stats_file" "$query_args_str"

    for batch_idx in $(seq 1 "$batch_count"); do
      batch_path="$(printf "%s/batch_%02d" "$batch_root" "$batch_idx")"
      echo "[STEP] Apply batch ${batch_idx}/${batch_count} to layout=${layout_token}"
      "$SPARK_HOME/bin/spark-submit" \
        --packages "$HUDI_PKG" \
        "$HUDI_UPSERT_SCRIPT" \
        --input "$batch_path" \
        --table-path "$table_path" \
        --table-name "$table_name" \
        --record-key "$record_key" \
        --precombine-field "$precombine" \
        --partition-field "$partition" \
        --operation upsert \
        --sort-columns "$sort_cols" \
        --target-file-mb "$target_mb" \
        --app-name "rq7-upsert-${layout_token}" \
        --shuffle-partitions 200

      echo "[STEP] Queries after batch ${batch_idx} layout=${layout_token}"
      run_queries_after_batch "$layout_token" "$batch_idx" "$name" "$base_scale" "$stats_file" "$query_args_str"
    done
  done
done

echo "[DONE] RQ7 incremental update matrix complete."
