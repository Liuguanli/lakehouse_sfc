#!/usr/bin/env bash
set -euo pipefail

# Ingest-only runner for TPCH scale=1 on Hudi:
# - default sort columns: l_suppkey,l_commitdate
# - default layouts: no_layout,linear,zorder,hilbert
# - no query execution

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HUDI_WRAPPER="${ROOT_DIR}/scripts/load_data_spec/run_hudi_layouts_tpch.sh"

[[ -f "${HUDI_WRAPPER}" ]] || { echo "Missing script: ${HUDI_WRAPPER}" >&2; exit 1; }

# Align with other runners: load lakehouse runtime env when available.
[[ -f "${HOME}/.lakehouse/env" ]] && source "${HOME}/.lakehouse/env"
[[ -f "${ROOT_DIR}/lakehouse.env" ]] && source "${ROOT_DIR}/lakehouse.env"

SCALE="${SCALE:-1}"
INPUT_TEMPLATE="${INPUT_TEMPLATE:-/datasets/tpch_%s.parquet}"
BASE_TEMPLATE_DEFAULT="./data/tpch_%s/hudi"
if [[ -d /datasets && -w /datasets ]]; then
  BASE_TEMPLATE_DEFAULT="/datasets/lakehouse_data/tpch_%s/hudi"
fi
BASE_TEMPLATE="${BASE_TEMPLATE:-$BASE_TEMPLATE_DEFAULT}"

LAYOUTS="${LAYOUTS:-no_layout,linear,zorder,hilbert}"
SORT_COLUMNS="${SORT_COLUMNS:-l_suppkey,l_commitdate}"
RECORD_KEY="${RECORD_KEY:-l_orderkey,l_linenumber}"
PRECOMBINE_FIELD="${PRECOMBINE_FIELD:-l_commitdate}"
PARTITION_FIELD="${PARTITION_FIELD:-l_returnflag,l_linestatus}"
TARGET_FILE_MB="${TARGET_FILE_MB:-128}"
OVERWRITE="${OVERWRITE:-0}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-120}"
MIN_FREE_GB="${MIN_FREE_GB:-20}"
SPARK_LOCAL_DIRS_PATH="${SPARK_LOCAL_DIRS_PATH:-}"

if [[ -z "${SPARK_LOCAL_DIRS_PATH}" && -d /datasets && -w /datasets ]]; then
  SPARK_LOCAL_DIRS_PATH="/datasets/.spark_local_${USER}/lakehouse"
fi

usage() {
  cat <<'USAGE'
run_tpch_hudi_ingest_scale1.sh
  Ingest TPCH scale=1 parquet into 4 Hudi layouts.
  This script only writes data and does not run any query.

Options:
  --scale N               Scale factor (default: 1)
  --input-template STR    Input template with %s scale slot (default: /datasets/tpch_%s.parquet)
  --base-template STR     Base dir template with %s scale slot
                          (default: /datasets/lakehouse_data/tpch_%s/hudi when writable; else ./data/tpch_%s/hudi)
  --layouts LIST          Layouts (default: no_layout,linear,zorder,hilbert)
  --sort-columns COLS     Sort columns (default: l_suppkey,l_commitdate)
  --record-key COLS       Hudi record key (default: l_orderkey,l_linenumber)
  --precombine-field COL  Precombine field (default: l_commitdate)
  --partition-field COLS  Partition field(s) (default: l_returnflag,l_linestatus)
  --target-file-mb N      Target file size MB (default: 128)
  --shuffle N             spark.sql.shuffle.partitions passed to core runner (default: 120)
  --spark-local-dirs DIR  Override SPARK_LOCAL_DIRS (default: /datasets/.spark_local_$USER/lakehouse when writable)
  --min-free-gb N         Minimum free GB required on output/tmp filesystems (default: 20)
  --overwrite             Remove base dir before ingest
  -h, --help              Show help

Env overrides:
  SCALE INPUT_TEMPLATE BASE_TEMPLATE LAYOUTS SORT_COLUMNS RECORD_KEY PRECOMBINE_FIELD
  PARTITION_FIELD TARGET_FILE_MB OVERWRITE
  SPARK_SHUFFLE_PARTITIONS SPARK_LOCAL_DIRS_PATH MIN_FREE_GB
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scale) SCALE="$2"; shift 2;;
    --input-template) INPUT_TEMPLATE="$2"; shift 2;;
    --base-template) BASE_TEMPLATE="$2"; shift 2;;
    --layouts) LAYOUTS="$2"; shift 2;;
    --sort-columns) SORT_COLUMNS="$2"; shift 2;;
    --record-key) RECORD_KEY="$2"; shift 2;;
    --precombine-field) PRECOMBINE_FIELD="$2"; shift 2;;
    --partition-field) PARTITION_FIELD="$2"; shift 2;;
    --target-file-mb) TARGET_FILE_MB="$2"; shift 2;;
    --shuffle) SPARK_SHUFFLE_PARTITIONS="$2"; shift 2;;
    --spark-local-dirs) SPARK_LOCAL_DIRS_PATH="$2"; shift 2;;
    --min-free-gb) MIN_FREE_GB="$2"; shift 2;;
    --overwrite) OVERWRITE="1"; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown option: $1" >&2; usage; exit 2;;
  esac
done

safe_rm() {
  local p="$1"
  case "$p" in
    ./data/tpch_*/*|data/tpch_*/*|/datasets/lakehouse_data/tpch_*/*)
      rm -rf "$p"
      ;;
    *)
      echo "Refusing to delete outside allowed roots (./data/tpch_*/* or /datasets/lakehouse_data/tpch_*/*): $p" >&2
      exit 2
      ;;
  esac
}

INPUT_PATH="$(printf "$INPUT_TEMPLATE" "$SCALE")"
BASE_DIR="$(printf "$BASE_TEMPLATE" "$SCALE")"

if [[ "$OVERWRITE" == "1" ]]; then
  echo "[overwrite] removing ${BASE_DIR}"
  safe_rm "${BASE_DIR}"
fi

if [[ -n "${SPARK_LOCAL_DIRS_PATH}" ]]; then
  mkdir -p "${SPARK_LOCAL_DIRS_PATH}"
  export SPARK_LOCAL_DIRS="${SPARK_LOCAL_DIRS_PATH}"
fi

if ! command -v spark-submit >/dev/null 2>&1; then
  echo "ERROR: spark-submit not found." >&2
  echo "Hint: ensure ${HOME}/.lakehouse/env exists and is source-able, or export SPARK_HOME/bin into PATH." >&2
  exit 1
fi

free_gb_for_path() {
  local path="$1"
  df -Pk "$path" | awk 'NR==2 {printf "%d", $4/1024/1024}'
}

require_free_space() {
  local path="$1"
  local label="$2"
  local free_gb
  free_gb="$(free_gb_for_path "$path")"
  if [[ "$free_gb" -lt "$MIN_FREE_GB" ]]; then
    echo "ERROR: low free space on ${label} filesystem (${free_gb} GB < ${MIN_FREE_GB} GB)." >&2
    echo "This likely causes Hudi failures like: No space left on device." >&2
    exit 1
  fi
}

mkdir -p "$(dirname "${BASE_DIR}")"
require_free_space "$(dirname "${BASE_DIR}")" "base-dir"
if [[ -n "${SPARK_LOCAL_DIRS:-}" ]]; then
  require_free_space "${SPARK_LOCAL_DIRS}" "SPARK_LOCAL_DIRS"
fi

echo "Scale          : ${SCALE}"
echo "Input          : ${INPUT_PATH}"
echo "Base dir       : ${BASE_DIR}"
echo "Layouts        : ${LAYOUTS}"
echo "Sort columns   : ${SORT_COLUMNS}"
echo "Record key     : ${RECORD_KEY}"
echo "Precombine     : ${PRECOMBINE_FIELD}"
echo "Partition field: ${PARTITION_FIELD}"
echo "Target file MB : ${TARGET_FILE_MB}"
echo "Shuffle parts  : ${SPARK_SHUFFLE_PARTITIONS}"
echo "SPARK_LOCAL_DIRS: ${SPARK_LOCAL_DIRS:-<unset>}"
echo "Min free GB    : ${MIN_FREE_GB}"
echo "Mode           : ingest-only (no queries)"

bash "${HUDI_WRAPPER}" \
  --scales "${SCALE}" \
  --input-template "${INPUT_TEMPLATE}" \
  --base-template "${BASE_TEMPLATE}" \
  --layouts "${LAYOUTS}" \
  --record-key "${RECORD_KEY}" \
  --precombine-field "${PRECOMBINE_FIELD}" \
  --partition-field "${PARTITION_FIELD}" \
  --sort-columns "${SORT_COLUMNS}" \
  --target-file-mb "${TARGET_FILE_MB}" \
  -- \
  --shuffle "${SPARK_SHUFFLE_PARTITIONS}"

echo "[DONE] TPCH Hudi ingest completed."
