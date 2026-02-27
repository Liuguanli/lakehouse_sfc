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
# DEFAULT_BASE_TEMPLATE="./data/tpch_%s/hudi"
DEFAULT_BASE_TEMPLATE="${ROOT_DIR}/data/tpch_%s/hudi"

HUDI_PKG="${HUDI_PKG:-org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2}"

# Upsert stability knobs (env-overridable)
RQ7_UPSERT_DRIVER_MEMORY="${RQ7_UPSERT_DRIVER_MEMORY:-16g}"
RQ7_UPSERT_EXECUTOR_MEMORY="${RQ7_UPSERT_EXECUTOR_MEMORY:-16g}"
RQ7_UPSERT_MEMORY_OVERHEAD="${RQ7_UPSERT_MEMORY_OVERHEAD:-4g}"
RQ7_UPSERT_SHUFFLE_PARTITIONS="${RQ7_UPSERT_SHUFFLE_PARTITIONS:-64}"
RQ7_UPSERT_COALESCE="${RQ7_UPSERT_COALESCE:-64}"
RQ7_UPSERT_SMALL_FILE_LIMIT_MB="${RQ7_UPSERT_SMALL_FILE_LIMIT_MB:-64}"
RQ7_UPSERT_MAX_NUM_GROUPS="${RQ7_UPSERT_MAX_NUM_GROUPS:-512}"
RQ7_UPSERT_METADATA_ENABLE="${RQ7_UPSERT_METADATA_ENABLE:-false}"

# Test overrides (env-overridable)
RQ7_QUERY_ARGS_OVERRIDE="${RQ7_QUERY_ARGS_OVERRIDE:-}"
RQ7_LAYOUTS_OVERRIDE="${RQ7_LAYOUTS_OVERRIDE:-}"
RQ7_BATCHES_OVERRIDE="${RQ7_BATCHES_OVERRIDE:-}"
RQ7_AUTO_BUILD_BASE_IF_MISSING="${RQ7_AUTO_BUILD_BASE_IF_MISSING:-1}"

# ---------------------------------------------------------------------------
# Scenario definition
# ---------------------------------------------------------------------------
declare -A SCENARIO_RQ7_DEFAULT=(
  [name]="SCENARIO_DEFAULT"
  [base_scale]=16
  [source_scale]=4
  [layouts]="no_layout"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_receiptdate"
  [target_mb]="128"
  [upsert_shuffle_partitions]="64"
  [upsert_coalesce]="64"
  [upsert_small_file_limit_mb]="64"
  [upsert_max_num_groups]="512"
  [batches]=10              # 10% increments of TPCH_4
  [shuffle_seed]=42                  
  [query_args]="--workload-type custom --spec-dir ${ROOT_DIR}/workload_spec/tpch_rq7 --spec-glob spec_tpch_*.yaml"
)

declare -A SCENARIO_RQ7_DEFAULT_1=(
  [name]="SCENARIO_DEFAULT"
  [base_scale]=16
  [source_scale]=4
  [layouts]="linear"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_receiptdate"
  [target_mb]="128"
  [upsert_shuffle_partitions]="64"
  [upsert_coalesce]="64"
  [upsert_small_file_limit_mb]="64"
  [upsert_max_num_groups]="512"
  [batches]=10              # 10% increments of TPCH_4
  [shuffle_seed]=42                  
  [query_args]="--workload-type custom --spec-dir ${ROOT_DIR}/workload_spec/tpch_rq7 --spec-glob spec_tpch_*.yaml"
)

declare -A SCENARIO_RQ7_DEFAULT_2=(
  [name]="SCENARIO_DEFAULT"
  [base_scale]=16
  [source_scale]=4
  [layouts]="zorder"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_receiptdate"
  [target_mb]="128"
  [upsert_shuffle_partitions]="64"
  [upsert_coalesce]="64"
  [upsert_small_file_limit_mb]="64"
  [upsert_max_num_groups]="512"
  [batches]=10              # 10% increments of TPCH_4
  [shuffle_seed]=42                  
  [query_args]="--workload-type custom --spec-dir ${ROOT_DIR}/workload_spec/tpch_rq7 --spec-glob spec_tpch_*.yaml"
)

declare -A SCENARIO_RQ7_DEFAULT_3=(
  [name]="SCENARIO_DEFAULT"
  [base_scale]=16
  [source_scale]=4
  [layouts]="hilbert"
  [record_key]="l_orderkey,l_linenumber"
  [precombine]="l_commitdate"
  [partition]="l_returnflag,l_linestatus"
  [sort]="l_shipdate,l_receiptdate"
  [target_mb]="128"
  [upsert_shuffle_partitions]="64"
  [upsert_coalesce]="64"
  [upsert_small_file_limit_mb]="64"
  [upsert_max_num_groups]="512"
  [batches]=10              # 10% increments of TPCH_4
  [shuffle_seed]=42                  
  [query_args]="--workload-type custom --spec-dir ${ROOT_DIR}/workload_spec/tpch_rq7 --spec-glob spec_tpch_*.yaml"
)

# declare -A SCENARIO_RQ7_L1=(
#   [name]="SCENARIO_DEFAULT_V1"
#   [base_scale]=16
#   [source_scale]=4
#   [layouts]="linear"
#   [record_key]="l_orderkey,l_linenumber"
#   [precombine]="l_commitdate"
#   [partition]="l_returnflag,l_linestatus"
#   [sort]="l_shipdate,l_receiptdate"
#   [target_mb]="128"
#   [batches]=10              # 10% increments of TPCH_4
#   [shuffle_seed]=42
#   [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq7 --spec-glob spec_tpch_*.yaml"
# )

# declare -A SCENARIO_RQ7_L2=(
#   [name]="SCENARIO_DEFAULT_V2"
#   [base_scale]=16
#   [source_scale]=4
#   [layouts]="zorder"
#   [record_key]="l_orderkey,l_linenumber"
#   [precombine]="l_commitdate"
#   [partition]="l_returnflag,l_linestatus"
#   [sort]="l_shipdate,l_receiptdate"
#   [target_mb]="128"
#   [batches]=10              # 10% increments of TPCH_4
#   [shuffle_seed]=42
#   [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq7 --spec-glob spec_tpch_*.yaml"
# )

# declare -A SCENARIO_RQ7_L3=(
#   [name]="SCENARIO_DEFAULT_V3"
#   [base_scale]=16
#   [source_scale]=4
#   [layouts]="hilbert"
#   [record_key]="l_orderkey,l_linenumber"
#   [precombine]="l_commitdate"
#   [partition]="l_returnflag,l_linestatus"
#   [sort]="l_shipdate,l_receiptdate"
#   [target_mb]="128"
#   [batches]=10              # 10% increments of TPCH_4
#   [shuffle_seed]=42
#   [query_args]="--workload-type custom --spec-dir workload_spec/tpch_rq7 --spec-glob spec_tpch_*.yaml"
# )

# SCENARIOS=(SCENARIO_RQ7_DEFAULT)
SCENARIOS=(SCENARIO_RQ7_DEFAULT_3) # SCENARIO_RQ7_L1 SCENARIO_RQ7_L2 SCENARIO_RQ7_L3)

run_queries_after_batch() {
  local layout="$1" batch_label="$2" name="$3" base_scale="$4" stats_file="$5" query_args_str="$6"
  local output_root="${ROOT_DIR}/workloads/tpch_rq7"
  local sql_root="${output_root}/sql"
  mkdir -p "$output_root" "$sql_root"

  read -r -a query_args <<<"$query_args_str"
  env \
    HUDI_LAYOUTS="$layout" \
    RESULT_FILE_SUFFIX="batch_${batch_label}" \
    RESULTS_SUBDIR_OVERRIDE="batch_${batch_label}" \
    bash "${ROOT_DIR}/scripts/run_RQ_query.sh" \
    --dataset-name "tpch_${base_scale}" \
    --output-root "$output_root" \
    --sql-root "$sql_root" \
    --stats-file "$stats_file" \
    --tag "scenario=${name}" \
    "${query_args[@]}"
}

ensure_base_layout_exists() {
  local layout_token="$1" base_scale="$2" base_template="$3" input_template="$4"
  local record_key="$5" precombine="$6" partition="$7" sort_cols="$8" target_mb="$9"
  local base_dir table_path
  base_dir=$(printf "$base_template" "$base_scale")
  table_path="${base_dir}/hudi_${layout_token}"

  if [[ -d "$table_path" ]]; then
    return 0
  fi

  if [[ "$RQ7_AUTO_BUILD_BASE_IF_MISSING" == "1" ]]; then
    echo "[WARN] Missing table path: ${table_path}"
    echo "[STEP] Auto-build base layout=${layout_token} scale=${base_scale}"
    (
      export HUDI_BASE_TEMPLATE="$base_template"
      export HUDI_INPUT_TEMPLATE="$input_template"
      cmd=( "$RUN_RQ1"
            --scales "$base_scale"
            --layouts "$layout_token"
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
    return 0
  fi

  echo "[ERROR] Missing Hudi table path: ${table_path}" >&2
  echo "[HINT] Build base first, e.g.:" >&2
  echo "  bash scripts/run_RQ_1.sh --scales ${base_scale} --layouts ${layout_token} --dataset tpch --skip-query" >&2
  echo "Or rerun with auto build:" >&2
  echo "  RQ7_AUTO_BUILD_BASE_IF_MISSING=1 bash scripts/run_RQ_7_matrix.sh" >&2
  return 1
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
  upsert_shuffle_partitions="${scenario[upsert_shuffle_partitions]:-${RQ7_UPSERT_SHUFFLE_PARTITIONS}}"
  upsert_coalesce="${scenario[upsert_coalesce]:-${RQ7_UPSERT_COALESCE}}"
  upsert_small_file_limit_mb="${scenario[upsert_small_file_limit_mb]:-${RQ7_UPSERT_SMALL_FILE_LIMIT_MB}}"
  upsert_max_num_groups="${scenario[upsert_max_num_groups]:-${RQ7_UPSERT_MAX_NUM_GROUPS}}"
  batch_count="${scenario[batches]:-10}"
  seed="${scenario[shuffle_seed]:-42}"
  input_template="${scenario[input_template]:-${HUDI_INPUT_TEMPLATE:-$DEFAULT_INPUT_TEMPLATE}}"
  base_template="${scenario[base_template]:-${HUDI_BASE_TEMPLATE:-$DEFAULT_BASE_TEMPLATE}}"
  query_args_str="${scenario[query_args]:---workload-type custom --spec-dir ${ROOT_DIR}/workload_spec/tpch_rq7 --spec-glob spec_tpch_*.yaml}"

  [[ -n "$RQ7_LAYOUTS_OVERRIDE" ]] && layouts="$RQ7_LAYOUTS_OVERRIDE"
  [[ -n "$RQ7_BATCHES_OVERRIDE" ]] && batch_count="$RQ7_BATCHES_OVERRIDE"
  [[ -n "$RQ7_QUERY_ARGS_OVERRIDE" ]] && query_args_str="$RQ7_QUERY_ARGS_OVERRIDE"

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
  echo "Query args : ${query_args_str}"
  echo "Upsert conf: shuffle=${upsert_shuffle_partitions}, coalesce=${upsert_coalesce}, small_file_limit_mb=${upsert_small_file_limit_mb}, max_num_groups=${upsert_max_num_groups}, metadata_enable=${RQ7_UPSERT_METADATA_ENABLE}"
  echo "Auto-build : ${RQ7_AUTO_BUILD_BASE_IF_MISSING}"

  # echo "[STEP] Clean base data scale=${base_scale}"
  # bash "$CLEAN_SCRIPT" --yes --scales "$base_scale"

  # echo "[STEP] Build base TPCH_${base_scale} layouts"
  # (
  #   export HUDI_BASE_TEMPLATE="$base_template"
  #   export HUDI_INPUT_TEMPLATE="$input_template"
  #   cmd=( "$RUN_RQ1"
  #         --scales "$base_scale"
  #         --layouts "$layouts"
  #         --dataset tpch
  #         --dataset-name "tpch_${base_scale}"
  #         --skip-query )
  #   [[ -n "$record_key" ]] && cmd+=(--record-key "$record_key")
  #   [[ -n "$precombine" ]] && cmd+=(--precombine-field "$precombine")
  #   [[ -n "$partition" ]] && cmd+=(--partition-field "$partition")
  #   [[ -n "$sort_cols" ]] && cmd+=(--sort-columns "$sort_cols")
  #   [[ -n "$target_mb" ]] && cmd+=(--target-file-mb "$target_mb")
  #   bash "${cmd[@]}"
  # )

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
    ensure_base_layout_exists "$layout_token" "$base_scale" "$base_template" "$input_template" \
      "$record_key" "$precombine" "$partition" "$sort_cols" "$target_mb"
    table_path="${base_dir}/hudi_${layout_token}"
    table_name="events_hudi_${layout_token}"

    echo "[STEP] Baseline queries layout=${layout_token} (before inserts)"
    run_queries_after_batch "$layout_token" 0 "$name" "$base_scale" "$stats_file" "$query_args_str"

    for batch_idx in $(seq 1 "$batch_count"); do
      batch_path="$(printf "%s/batch_%02d" "$batch_root" "$batch_idx")"
      echo "[STEP] Apply batch ${batch_idx}/${batch_count} to layout=${layout_token}"
      "$SPARK_HOME/bin/spark-submit" \
        --packages "$HUDI_PKG" \
        --conf "spark.driver.memory=${RQ7_UPSERT_DRIVER_MEMORY}" \
        --conf "spark.executor.memory=${RQ7_UPSERT_EXECUTOR_MEMORY}" \
        --conf "spark.executor.memoryOverhead=${RQ7_UPSERT_MEMORY_OVERHEAD}" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --conf "spark.kryoserializer.buffer=16m" \
        --conf "spark.kryoserializer.buffer.max=256m" \
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
        --small-file-limit-mb "$upsert_small_file_limit_mb" \
        --max-num-groups "$upsert_max_num_groups" \
        --coalesce "$upsert_coalesce" \
        --metadata-enable "$RQ7_UPSERT_METADATA_ENABLE" \
        --app-name "rq7-upsert-${layout_token}" \
        --shuffle-partitions "$upsert_shuffle_partitions"

      echo "[STEP] Queries after batch ${batch_idx} layout=${layout_token}"
      run_queries_after_batch "$layout_token" "$batch_idx" "$name" "$base_scale" "$stats_file" "$query_args_str"
    done
  done
done

echo "[DONE] RQ7 incremental update matrix complete."
