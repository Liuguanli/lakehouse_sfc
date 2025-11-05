#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Usage:
#   bash run_tpch_query.sh                  # Run for all default scales (1 4 16 64)
#   bash run_tpch_query.sh 1 4              # Run for selected scales only
#   bash run_tpch_query.sh --force 4 16     # Force regeneration for specific scales
#   bash run_tpch_query.sh --clean 1        # Clean old outputs before rebuilding
#   bash run_tpch_query.sh -f               # Force regeneration for all
#
# Flags:
#   -f, --force : Force overwrite existing outputs
#       --clean : Remove old stats/yaml/sql directories before rebuild
# ============================================================

FORCE=0
CLEAN=0
_scales=()

# --- Parse CLI flags and scales ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--force) FORCE=1; shift;;
    --clean)    CLEAN=1; shift;;
    -h|--help)
      sed -n '1,30p' "$0"; exit 0;;
    *) _scales+=("$1"); shift;;
  esac
done

if [[ ${#_scales[@]} -eq 0 ]]; then
  _scales=(1 4 16 64)
fi

DATASET_DIR="/datasets"
SPEC_DIR="workload_spec"
YAML_OUT_DIR="workloads/yaml"
SQL_BASE_DIR="workloads"
STATS_OUT_DIR="workloads/stats"
RUNNER="./scripts/run_query.sh"

mkdir -p "$YAML_OUT_DIR" "$STATS_OUT_DIR" "$SQL_BASE_DIR"

# Load Spark/Lakehouse environment if available
[[ -f "${HOME}/.lakehouse/env" ]] && source "${HOME}/.lakehouse/env"

# ============================================================
# Helper functions
# ============================================================

# Remove all generated files for a given scale
clean_scale_outputs() {
  local s="$1"
  echo "[CLEAN] Removing old outputs for tpch_${s}"
  rm -f  "${STATS_OUT_DIR}/tpch_${s}_stats.yaml" || true
  rm -f  "${YAML_OUT_DIR}/tpch_${s}"_q*.yaml || true
  rm -rf "${SQL_BASE_DIR}/tpch_${s}_Q"* || true
}

# Generate stats file unless it already exists (or FORCE=1)
profile_stats() {
  local s="$1"
  local parquet="${DATASET_DIR}/tpch_${s}.parquet"
  local stats="${STATS_OUT_DIR}/tpch_${s}_stats.yaml"

  [[ -f "$parquet" ]] || { echo "[WARN] Missing dataset: $parquet. Skipping scale ${s}."; return 1; }

  if [[ "$FORCE" != "1" && -s "$stats" ]]; then
    echo "[SKIP] Stats already exist: $stats"
    return 0
  fi

  echo ">> Profiling dataset tpch_${s} -> ${stats}"
  python -m wlg.cli profile \
    --input "$parquet" \
    --format parquet \
    --out "$stats" \
    --sample-rows 200000 \
    --seed 42 \
    --infer-dates
  echo "[OK] Stats generated: ${stats}"
}

# Choose workload spec (prefer scale-specific, else fallback)
pick_spec() {
  local s="$1" q="$2"
  local a="${SPEC_DIR}/spec_tpch_${s}_Q${q}.yaml"
  local b="${SPEC_DIR}/spec_tpch_Q${q}.yaml"
  [[ -f "$a" ]] && echo "$a" && return 0
  [[ -f "$b" ]] && echo "$b" && return 0
  echo ""
}

# Check if SQL directory already contains any .sql files
have_sql_files() { compgen -G "$1/*.sql" >/dev/null 2>&1; }

# Generate YAML + SQL for one query, and optionally run it
fill_one_query() {
  local s="$1" q="$2" stats="$3"
  local spec
  spec="$(pick_spec "$s" "$q")"
  [[ -n "$spec" ]] || { echo "[WARN] Spec not found for scale=${s}, Q${q}"; return 1; }

  local sql_dir="${SQL_BASE_DIR}/tpch_${s}_Q${q}"
  local yaml_out="${YAML_OUT_DIR}/tpch_${s}_q${q}.yaml"
  mkdir -p "$sql_dir"

  # Skip regeneration if files already exist (unless FORCE=1)
  if [[ "${FORCE:-0}" != "1" && -s "$yaml_out" ]] && have_sql_files "$sql_dir"; then
    echo "[SKIP] Query Q${q} (scale ${s}) already exists: ${yaml_out}, ${sql_dir}"
  else
    echo ">> Generating Q${q} (scale ${s}) using spec: ${spec}"
    python -m wlg.cli fill \
      --spec "$spec" \
      --stats "$stats" \
      --out "$yaml_out" \
      --sql-dir "$sql_dir"
    echo "[OK] YAML and SQL generated: ${yaml_out}, ${sql_dir}"
  fi

  # Optionally run the queries via the runner script
  if [[ -f "$RUNNER" ]]; then
    echo ">> Running workload: dataset=tpch_${s}, sql_dir=${sql_dir}"
    bash "$RUNNER" "tpch_${s}" "$sql_dir" ${RUNNER_ARGS:-}
  else
    echo "[WARN] Runner not found: $RUNNER. Skipping execution."
  fi
}

# ============================================================
# Main loop
# ============================================================

for S in "${_scales[@]}"; do
  echo "==== Processing scale: tpch_${S} ===="
  [[ "$CLEAN" == "1" ]] && clean_scale_outputs "$S"

  profile_stats "$S" || continue
  STATS_FILE="${STATS_OUT_DIR}/tpch_${S}_stats.yaml"

  for q in $(seq 1 7); do
    fill_one_query "$S" "$q" "$STATS_FILE" || true
  done
done

echo "[DONE] All selected scales processed successfully."
