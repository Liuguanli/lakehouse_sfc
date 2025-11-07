#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Amazon reviews query generator + runner
#
# Usage examples:
#   bash scripts/query_data_spec/run_amazon_query.sh
#   bash scripts/query_data_spec/run_amazon_query.sh --force
#   bash scripts/query_data_spec/run_amazon_query.sh --no-run
#   bash scripts/query_data_spec/run_amazon_query.sh --input /custom/path.csv
#
# Flags:
#   --force   : regenerate stats / workloads even if they already exist
#   --clean   : delete existing stats, YAML, and SQL outputs before rebuilding
#   --no-run  : generate YAML + SQL but skip executing the workloads
#   --input   : override dataset location (default: /datasets/amazon_review_all.csv)
# ============================================================

FORCE=0
CLEAN=0
RUN_QUERIES=1
INPUT="${INPUT:-/datasets/amazon_review_all.csv}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=1; shift;;
    --clean) CLEAN=1; shift;;
    --no-run) RUN_QUERIES=0; shift;;
    --input) INPUT="$2"; shift 2;;
    -h|--help)
      sed -n '1,40p' "$0"; exit 0;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2;;
  esac
done

# ------------------------------------------------------------
# Paths / constants
# ------------------------------------------------------------
DATASET_NAME="amazon"
SPEC_IDS=(Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10 Q11 Q12)

STATS_DIR="workloads/stats"
YAML_DIR="workloads/yaml"
SQL_BASE_DIR="workloads"
RUNNER="./scripts/run_query.sh"
STATS_FILE="${STATS_DIR}/amazon_stats.yaml"

mkdir -p "$STATS_DIR" "$YAML_DIR" "$SQL_BASE_DIR"

[[ -f "${HOME}/.lakehouse/env" ]] && source "${HOME}/.lakehouse/env"

# ------------------------------------------------------------
clean_outputs() {
  echo "[CLEAN] Removing Amazon outputs"
  rm -f "$STATS_FILE" || true
  for q in "${SPEC_IDS[@]}"; do
    rm -f "${YAML_DIR}/amazon_${q,,}.yaml" || true
    rm -rf "${SQL_BASE_DIR}/amazon_${q}" || true
  done
}

profile_stats() {
  if [[ ! -f "$INPUT" ]]; then
    echo "[WARN] Dataset not found: $INPUT"
    return 1
  fi
  if [[ "$FORCE" != "1" && -s "$STATS_FILE" ]]; then
    echo "[SKIP] Stats already exist: $STATS_FILE"
    return 0
  fi
  echo ">> Profiling Amazon dataset -> ${STATS_FILE}"
  python -m wlg.cli profile \
    --input "$INPUT" \
    --format csv \
    --out "$STATS_FILE" \
    --sample-rows 200000 \
    --seed 42 \
    --infer-dates
  echo "[OK] Stats generated: $STATS_FILE"
}

have_sql_files() { compgen -G "$1/*.sql" >/dev/null 2>&1; }

fill_one_query() {
  local qid="$1"
  local spec="workload_spec/spec_amazon_${qid}.yaml"
  local sql_dir="${SQL_BASE_DIR}/amazon_${qid}"
  local yaml_out="${YAML_DIR}/amazon_${qid,,}.yaml"

  if [[ ! -f "$spec" ]]; then
    echo "[WARN] Spec not found: ${spec}"
    return 1
  fi
  mkdir -p "$sql_dir"

  if [[ "${FORCE}" != "1" && -s "$yaml_out" ]] && have_sql_files "$sql_dir"; then
    echo "[SKIP] ${qid} already materialised: ${yaml_out}, ${sql_dir}"
  else
    echo ">> Generating workload ${qid} using ${spec}"
    python -m wlg.cli fill \
      --spec "$spec" \
      --stats "$STATS_FILE" \
      --out "$yaml_out" \
      --sql-dir "$sql_dir"
    echo "[OK] YAML and SQL generated: ${yaml_out}, ${sql_dir}"
  fi

  if [[ "$RUN_QUERIES" == "1" ]]; then
    if [[ -f "$RUNNER" ]]; then
      echo ">> Running workload ${qid}"
      local -a extra_args=()
      if [[ -n ${RUNNER_ARGS:-} ]]; then
        # shellcheck disable=SC2206
        extra_args=(${RUNNER_ARGS})
      fi
      bash "$RUNNER" "$DATASET_NAME" "$sql_dir" "${extra_args[@]}"
    else
      echo "[WARN] Runner not found: $RUNNER (skipping execution)"
    fi
  fi
}

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
[[ "$CLEAN" == "1" ]] && clean_outputs
profile_stats || exit 1

for q in "${SPEC_IDS[@]}"; do
  fill_one_query "$q" || true
done

echo "[DONE] Amazon workloads processed."
