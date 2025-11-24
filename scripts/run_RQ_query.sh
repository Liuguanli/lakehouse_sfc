#!/usr/bin/env bash
set -euo pipefail
#
# Drive the RQ1 workload:
#   1. Pick query specs from workload_spec/tpch_rq1/*.yaml (or a filtered subset).
#   2. Materialise SQL via wlg.cli fill using tpch_16 stats.
#   3. Execute the resulting workload directory via scripts/run_query.sh with
#      Hudi layouts (or user supplied layout lists).
#
# Usage examples:
#   bash scripts/run_RQ_query.sh                       # run every spec
#   bash scripts/run_RQ_query.sh --limit 10            # run first 10 specs only
#   bash scripts/run_RQ_query.sh --spec spec_tpch_RQ1_Q1_S1_C1_N2_O1.yaml
#   HUDI_LAYOUTS="no_layout,hilbert" bash scripts/run_RQ_query.sh --skip-run
#

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKLOAD_KIND="tpch"
CUSTOM_OUTPUT_ROOT="${RQ1_OUTPUT_ROOT:-}"
DATASET_NAME_OVERRIDE=""
SPEC_DIR=""
SPEC_DIR_OVERRIDE=""
OUTPUT_ROOT=""
SQL_ROOT=""
STATS_FILE=""
DATASET=""
RUNNER="${ROOT_DIR}/scripts/run_query.sh"

FORCE=0
SKIP_RUN=0
LIMIT=0
SPEC_GLOB="spec_tpch_RQ1_*.yaml"
SPEC_GLOB_OVERRIDDEN=0
declare -a SPEC_OVERRIDE=()
declare -a RUNNER_TAGS=()
START_AFTER=""
START_FOUND=0

HUDI_LAYOUTS="${HUDI_LAYOUTS:-no_layout,linear,zorder,hilbert}"
DELTA_LAYOUTS="${DELTA_LAYOUTS:-baseline,linear,zorder}"
ICEBERG_LAYOUTS="${ICEBERG_LAYOUTS:-baseline,linear,zorder}"
declare -a RUN_ENGINES=()

apply_workload_defaults() {
  # Defaults per workload type; can be overridden via --spec-dir/--spec-glob.
  case "$WORKLOAD_KIND" in
    tpch)
      local default_spec_dir="${ROOT_DIR}/workload_spec/tpch_rq1"
      local default_root="${ROOT_DIR}/workloads/tpch_rq1"
      STATS_FILE="${ROOT_DIR}/workloads/stats/tpch_16_stats.yaml"
      local default_dataset="tpch_16"
      local default_glob="spec_tpch_RQ1_*.yaml"
      ;;
    amazon)
      local default_spec_dir="${ROOT_DIR}/workload_spec/amazon_rq1"
      local default_root="${ROOT_DIR}/workloads/amazon_rq1"
      STATS_FILE="${ROOT_DIR}/workloads/stats/amazon_stats.yaml"
      local default_dataset="amazon"
      local default_glob="spec_amazon_RQ1_*.yaml"
      ;;
    custom)
      local default_spec_dir=""
      local default_root="${ROOT_DIR}/workloads/custom"
      STATS_FILE="${ROOT_DIR}/workloads/stats/tpch_16_stats.yaml"
      local default_dataset="tpch_16"
      local default_glob="spec_*.yaml"
      ;;
    *)
      echo "Unsupported workload type: ${WORKLOAD_KIND}" >&2
      exit 2
      ;;
  esac

  SPEC_DIR="${SPEC_DIR_OVERRIDE:-$default_spec_dir}"
  local root_choice="${CUSTOM_OUTPUT_ROOT:-$default_root}"
  OUTPUT_ROOT="$root_choice"
  SQL_ROOT="${OUTPUT_ROOT}/sql"
  DATASET="${DATASET_NAME_OVERRIDE:-$default_dataset}"
  if [[ $SPEC_GLOB_OVERRIDDEN -eq 0 ]]; then
    SPEC_GLOB="${SPEC_DIR_OVERRIDE:+spec_*.yaml}"
    [[ -z "$SPEC_GLOB" ]] && SPEC_GLOB="$default_glob"
  fi
}

apply_workload_defaults

usage() {
  cat <<'EOF'
run_RQ_query.sh [options] [-- extra run_query.sh args]
  --spec FILE                 Run a particular spec (under workload_spec/tpch_rq1)
  --spec-glob PATTERN         Glob for spec discovery (default: spec_tpch_RQ1_*.yaml)
  --limit N                   Only process the first N specs
  --start-after FILE          Skip specs until after FILE (basename or .yaml)
  --force                     Regenerate SQL even if already present
  --skip-run                  Only generate SQL; skip execution
  --delta / --iceberg         Include those engines when invoking run_query.sh
  --hudi-layouts LIST         Override HUDI layouts passed downstream
  --delta-layouts LIST        Override delta layouts passed downstream
  --iceberg-layouts LIST      Override iceberg layouts passed downstream
  --spec-dir DIR              Override spec directory (default depends on workload)
  --output-root DIR           Override output directory (default depends on workload)
  --workload-type NAME        Workload type: tpch (default) or amazon
  --dataset-name NAME         Dataset identifier passed to run_query.sh
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --spec)
      SPEC_OVERRIDE+=("$2"); shift 2;;
    --spec-glob)
      SPEC_GLOB="$2"; SPEC_GLOB_OVERRIDDEN=1; shift 2;;
    --limit)
      LIMIT="$2"; shift 2;;
    --start-after)
      START_AFTER="$2"; shift 2;;
    --force)
      FORCE=1; shift;;
    --skip-run)
      SKIP_RUN=1; shift;;
    --delta) RUN_ENGINES+=(--delta); shift;;
    --iceberg) RUN_ENGINES+=(--iceberg); shift;;
    --hudi-layouts)
      HUDI_LAYOUTS="$2"; shift 2;;
    --delta-layouts)
      DELTA_LAYOUTS="$2"; shift 2;;
    --iceberg-layouts)
      ICEBERG_LAYOUTS="$2"; shift 2;;
    --spec-dir)
      SPEC_DIR_OVERRIDE="$2"; shift 2;;
    --output-root)
      CUSTOM_OUTPUT_ROOT="$2"; shift 2;;
    --workload-type)
      WORKLOAD_KIND="$2"; shift 2;;
    --dataset-name)
      DATASET_NAME_OVERRIDE="$2"; shift 2;;
    --tag)
      RUNNER_TAGS+=("$2"); shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown option: $1" >&2; usage; exit 2;;
  esac
done

apply_workload_defaults

if [[ ${#RUN_ENGINES[@]} -eq 0 ]]; then
  RUN_ENGINES=(--hudi)
fi

if [[ ! -f "$STATS_FILE" ]]; then
  echo "Stats file not found: $STATS_FILE" >&2
  exit 1
fi

mkdir -p "$OUTPUT_ROOT" "$SQL_ROOT"

have_sql_files() {
  local dir="$1"
  shopt -s nullglob
  local files=("$dir"/*.sql)
  shopt -u nullglob
  [[ ${#files[@]} -gt 0 ]]
}

collect_specs() {
  if [[ ${#SPEC_OVERRIDE[@]} -gt 0 ]]; then
    for spec in "${SPEC_OVERRIDE[@]}"; do
      local path="$SPEC_DIR/$spec"
      if [[ -f "$path" ]]; then
        printf '%s\0' "$path"
      else
        echo "[WARN] Spec not found: $path" >&2
      fi
    done
  else
    find "$SPEC_DIR" -maxdepth 1 -type f -name "$SPEC_GLOB" -print0 | sort -z
  fi
}

run_fill() {
  local spec_path="$1"
  local base
  base="$(basename "${spec_path%.*}")"
  local sql_dir="${SQL_ROOT}/${base}"
  mkdir -p "$sql_dir"

  local sql_ready=0
  if have_sql_files "$sql_dir"; then
    sql_ready=1
  fi

  if [[ $FORCE -ne 1 && $sql_ready -eq 1 ]]; then
    echo "[SKIP] Existing workload: $base"
  else
    echo "[GEN] $base"
    python -m wlg.cli fill \
      --spec "$spec_path" \
      --stats "$STATS_FILE" \
      --sql-dir "$sql_dir"
  fi

  if [[ $SKIP_RUN -eq 0 ]]; then
    echo "[RUN] dataset=${DATASET}, spec=${base}"
    cmd=( "$RUNNER" "$DATASET" "$sql_dir"
          "${RUN_ENGINES[@]}"
          --hudi-layouts "$HUDI_LAYOUTS"
          --delta-layouts "$DELTA_LAYOUTS"
          --iceberg-layouts "$ICEBERG_LAYOUTS" )
    for tag in "${RUNNER_TAGS[@]}"; do
      cmd+=(--tag "$tag")
    done
    HUDI_LAYOUTS="$HUDI_LAYOUTS" \
    DELTA_LAYOUTS="$DELTA_LAYOUTS" \
    ICEBERG_LAYOUTS="$ICEBERG_LAYOUTS" \
      bash "${cmd[@]}"
  fi
}

count=0
start_gate=1
start_match=""
if [[ -n "$START_AFTER" ]]; then
  start_gate=0
  start_match="${START_AFTER%.yaml}.yaml"
fi

while IFS= read -r -d '' spec_file; do
  if [[ ! -f "$spec_file" ]]; then
    continue
  fi
  if [[ $start_gate -eq 0 ]]; then
    base="$(basename "$spec_file")"
    if [[ "$base" == "$start_match" ]]; then
      start_gate=1
      START_FOUND=1
      continue  # start *after* the matched file
    else
      continue
    fi
  fi
  run_fill "$spec_file"
  count=$((count + 1))
  if [[ $LIMIT -gt 0 && $count -ge $LIMIT ]]; then
    break
  fi
done < <(collect_specs)

if [[ -n "$START_AFTER" && $START_FOUND -eq 0 ]]; then
  echo "[WARN] --start-after target not found: $START_AFTER" >&2
fi

echo "[DONE] Processed ${count} spec(s)."
