#!/usr/bin/env bash
set -euo pipefail

# ---------------- Minimal knobs (env/CLI overridable) ----------------
: "${INPUT:=/datasets/tpch_16.parquet}"                  # Source Parquet path
: "${WAREHOUSE:=./data/tpch_16/iceberg_wh}"              # HadoopCatalog root
: "${NAMESPACE:=local.demo}"                             # e.g., local.demo
: "${BASE_NAME:=events_iceberg}"                         # Base table name
: "${LAYOUTS:=}"                                         # Accepts "baseline", "linear", "zorder" (first wins)
: "${ICEBERG_PKG:=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0}"

# Optional: a simple scaling knob that matters for write cost
: "${SPARK_SHUF:=400}"                                   # spark.sql.shuffle.partitions
: "${TARGET_FILE_MB:=128}"                               # Target file size in MB

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Core:
  --input PATH                 Input Parquet (default: \$INPUT)
  --warehouse DIR              Iceberg HadoopCatalog warehouse (default: \$WAREHOUSE)
  --namespace NAME             Catalog namespace (default: \$NAMESPACE)
  --base-name NAME             Base table name (default: \$BASE_NAME)

Layout selection (ONLY the first one is used):
  --layout NAME                One of: baseline | linear | zorder
  # or via env: LAYOUTS="baseline" or "linear,zorder" (first item wins)

Sizing:
  --shuffle N                  spark.sql.shuffle.partitions (default: \$SPARK_SHUF)
  --target-file-mb N           Target file size MB (default: \$TARGET_FILE_MB)

Examples:
  LAYOUTS=linear $(basename "$0")
  $(basename "$0") --layout zorder --shuffle 600
EOF
}

# ---------------- CLI parsing ----------------
LAYOUT_CLI=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)          INPUT="$2"; shift 2;;
    --warehouse)      WAREHOUSE="$2"; shift 2;;
    --namespace)      NAMESPACE="$2"; shift 2;;
    --base-name)      BASE_NAME="$2"; shift 2;;
    --layout)         LAYOUT_CLI="$2"; shift 2;;                 # CLI override for layout
    --shuffle)        SPARK_SHUF="$2"; shift 2;;
    --target-file-mb) TARGET_FILE_MB="$2"; shift 2;;
    -h|--help)        usage; exit 0;;
    *) echo "Unknown option: $1"; usage; exit 2;;
  esac
done

# ---------------- Derive the chosen layout ----------------
# Precedence: CLI --layout > env LAYOUTS
LAYOUT_RAW="${LAYOUT_CLI:-${LAYOUTS:-}}"

# Normalize: take first token before comma or space, lowercased
#   - "linear,zorder" -> "linear"
#   - "  ZORDER  "    -> "zorder"
LAYOUT_ONE="${LAYOUT_RAW%%,*}"                  # cut at first comma
LAYOUT_ONE="$(echo "$LAYOUT_ONE" | awk '{print $1}')"  # first space-separated token
LAYOUT_ONE="${LAYOUT_ONE,,}"                    # to lowercase

# ---------------- Helpers ----------------
# Convert "a,b" or "a b" into array (used for printing/human checks if needed)
_to_arr() { local s="${1//,/ }"; ARR=($s); }

# Detect SPARK_HOME if not set
if [[ -z "${SPARK_HOME:-}" ]]; then
  if command -v spark-submit >/dev/null 2>&1; then
    export SPARK_HOME="$(dirname "$(dirname "$(command -v spark-submit)")")"
  else
    echo "spark-submit not found. Set SPARK_HOME." >&2; exit 1
  fi
fi

mkdir -p "$WAREHOUSE"

COMMON_CONF=(
  --packages "$ICEBERG_PKG"
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.local.type=hadoop
  --conf "spark.sql.catalog.local.warehouse=$(python3 - <<'PY'
import os; print(os.path.abspath(os.environ["WAREHOUSE"]))
PY
)"
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUF}"
)

# run_job <layout: baseline|linear|zorder> <optimize: none|sort|zorder> <suffix>
run_job() {
  local layout="$1" optimize="$2" suffix="$3"
  local table_id="${NAMESPACE}.${BASE_NAME}_${suffix}"

  echo "=== Iceberg write: ${table_id} (layout=${layout}, optimize=${optimize}) ==="
  "$SPARK_HOME/bin/spark-submit" \
    "${COMMON_CONF[@]}" \
    ./lakehouse_op/iceberg_write_layout.py \
      --input "$INPUT" \
      --warehouse "$WAREHOUSE" \
      --table-identifier "$table_id" \
      --mode overwrite \
      --layout "$layout" \
      --target-file-mb "$TARGET_FILE_MB"
  echo "[OK] $table_id"
}

# ---------------- Dispatch by chosen layout ----------------
case "$LAYOUT_ONE" in
  "")
    echo "[INFO] No layout provided (via --layout or LAYOUTS). Nothing will run."
    ;;
  baseline)
    run_job "baseline" "none" "baseline"
    ;;
  linear)
    # For linear we use a post-write sort; the driver script may handle it internally.
    run_job "linear" "sort" "linear"
    ;;
  zorder|"z-order")
    run_job "zorder" "zorder" "zorder"
    ;;
  *)
    echo "WARN: unknown Iceberg layout '$LAYOUT_ONE' (skipped)"
    ;;
esac
