#!/usr/bin/env bash
set -euo pipefail

# Ingest-only runner for Amazon on Hudi:
# - sort columns: user_id,record_timestamp
# - layouts: no_layout,linear,zorder,hilbert
# - no query execution

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HUDI_WRAPPER="${ROOT_DIR}/scripts/load_data_spec/run_hudi_layouts_amazon.sh"

[[ -f "${HUDI_WRAPPER}" ]] || { echo "Missing script: ${HUDI_WRAPPER}" >&2; exit 1; }

# Align with other runners: load lakehouse runtime env when available.
[[ -f "${HOME}/.lakehouse/env" ]] && source "${HOME}/.lakehouse/env"
[[ -f "${ROOT_DIR}/lakehouse.env" ]] && source "${ROOT_DIR}/lakehouse.env"

INPUT="${INPUT:-/datasets/amazon_review_all.csv}"
BASE_DIR_DEFAULT="./data/amazon/hudi_user_time"
if [[ -d /datasets && -w /datasets ]]; then
  BASE_DIR_DEFAULT="/datasets/lakehouse_data/amazon/hudi_user_time"
fi
BASE_DIR="${BASE_DIR:-$BASE_DIR_DEFAULT}"
LAYOUTS="${LAYOUTS:-no_layout,linear,zorder,hilbert}"
SORT_COLUMNS="${SORT_COLUMNS:-user_id,record_timestamp}"
RECORD_KEY="${RECORD_KEY:-user_id,asin}"
PRECOMBINE_FIELD="${PRECOMBINE_FIELD:-record_timestamp}"
PARTITION_FIELD="${PARTITION_FIELD:-category}"
TARGET_FILE_MB="${TARGET_FILE_MB:-256}"
OVERWRITE="${OVERWRITE:-0}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-120}"
MIN_FREE_GB="${MIN_FREE_GB:-20}"
SPARK_LOCAL_DIRS_PATH="${SPARK_LOCAL_DIRS_PATH:-}"

if [[ -z "${SPARK_LOCAL_DIRS_PATH}" && -d /datasets && -w /datasets ]]; then
  SPARK_LOCAL_DIRS_PATH="/datasets/.spark_local_${USER}/lakehouse"
fi

usage() {
  cat <<'EOF'
run_amazon_hudi_ingest_user_time.sh
  Ingest Amazon CSV into 4 Hudi layouts with sort key user_id,record_timestamp.
  This script only writes data and does not run any query.

Options:
  --input PATH             Source CSV (default: /datasets/amazon_review_all.csv)
  --base-dir DIR           Output base dir (default: /datasets/lakehouse_data/amazon/hudi_user_time when writable; else ./data/amazon/hudi_user_time)
  --layouts LIST           Layouts (default: no_layout,linear,zorder,hilbert)
  --sort-columns COLS      Sort columns (default: user_id,record_timestamp)
  --record-key COLS        Hudi record key (default: user_id,asin)
  --precombine-field COL   Precombine field (default: record_timestamp)
  --partition-field COLS   Partition field(s) (default: category)
  --target-file-mb N       Target file size MB (default: 256)
  --shuffle N              spark.sql.shuffle.partitions passed to core runner (default: 120)
  --spark-local-dirs DIR   Override SPARK_LOCAL_DIRS (default: /datasets/.spark_local_$USER/lakehouse when writable)
  --min-free-gb N          Minimum free GB required on output/tmp filesystems (default: 20)
  --overwrite              Remove base dir before ingest
  -h, --help               Show help

Env overrides:
  INPUT BASE_DIR LAYOUTS SORT_COLUMNS RECORD_KEY PRECOMBINE_FIELD
  PARTITION_FIELD TARGET_FILE_MB OVERWRITE
  SPARK_SHUFFLE_PARTITIONS SPARK_LOCAL_DIRS_PATH MIN_FREE_GB
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input) INPUT="$2"; shift 2;;
    --base-dir) BASE_DIR="$2"; shift 2;;
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
    ./data/amazon/*|data/amazon/*|/datasets/lakehouse_data/amazon/*)
      rm -rf "$p"
      ;;
    *)
      echo "Refusing to delete outside allowed roots (./data/amazon/* or /datasets/lakehouse_data/amazon/*): $p" >&2
      exit 2
      ;;
  esac
}

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

echo "Input          : ${INPUT}"
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
  --input "${INPUT}" \
  --base-dir "${BASE_DIR}" \
  --layouts "${LAYOUTS}" \
  --record-key "${RECORD_KEY}" \
  --precombine-field "${PRECOMBINE_FIELD}" \
  --partition-field "${PARTITION_FIELD}" \
  --sort-columns "${SORT_COLUMNS}" \
  --target-file-mb "${TARGET_FILE_MB}" \
  -- \
  --shuffle "${SPARK_SHUFFLE_PARTITIONS}"

echo "[DONE] Hudi ingest completed."
