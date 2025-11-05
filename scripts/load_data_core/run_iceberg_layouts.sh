#!/usr/bin/env bash
set -euo pipefail

# ========= Defaults (overridable via env or CLI) =========
: "${INPUT:=/datasets/tpch_16.parquet}"         # Source Parquet to ingest
: "${WAREHOUSE:=./data/tpch_16/iceberg_wh}"     # HadoopCatalog warehouse root
: "${NAMESPACE:=local.demo}"                    # Iceberg namespace (catalog.db)
: "${BASE_NAME:=events_iceberg}"                # Base table name prefix
: "${LAYOUTS:=}"                                # Requested layout(s): baseline|linear|zorder (first wins)

: "${ICEBERG_PKG:=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0}"  # Iceberg runtime for Spark 3.5 / Scala 2.12

# Column selections (space or comma separated; normalized later)
: "${PARTITION_BY:=l_returnflag l_linestatus}"  # Iceberg partition columns
: "${RANGE_COLS:=l_shipdate l_receiptdate}"     # Optional range repartition columns (pre-write)
: "${LAYOUT_COLS:=l_shipdate l_receiptdate}"    # Columns used for linear/zorder layouts

# Spark resources and stability knobs (aligned with Delta/Hudi scripts)
: "${SPARK_SHUF:=400}"                          # spark.sql.shuffle.partitions
: "${DRIVER_MEM:=48g}"                          # Driver heap
: "${EXEC_MEM:=64g}"                            # Executor heap
: "${EXEC_OVH:=16g}"                            # Executor off-heap/overhead
: "${MAX_PART_BYTES:=256m}"                     # spark.sql.files.maxPartitionBytes (controls split size)
: "${ADAPTIVE:=true}"                           # Enable AQE
: "${VEC_READER:=true}"                         # Parquet vectorized reader
: "${PARQUET_BATCH:=1024}"                      # Vectorized reader batch size
: "${SESSION_TZ:=UTC}"                          # Session time zone (parsing/ casting)

# Advisory target file size for Iceberg writers
: "${TARGET_FILE_MB:=128}"

CONFIG_FILE=""

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Core:
  --input PATH                 Input Parquet (default: \$INPUT)
  --warehouse DIR              Iceberg HadoopCatalog warehouse (default: \$WAREHOUSE)
  --namespace NAME             Catalog namespace (default: \$NAMESPACE)
  --base-name NAME             Base table name (default: \$BASE_NAME)

Layout (only first token is used):
  --layout NAME                baseline | linear | zorder
  # or env: LAYOUTS="linear" / "linear,zorder"

Columns:
  --partition-by COLS          space/comma separated
  --range-cols COLS
  --layout-cols COLS

Sizing & Runtime:
  --shuffle N                  spark.sql.shuffle.partitions (default: \$SPARK_SHUF)
  --driver-mem SIZE            (default: \$DRIVER_MEM)
  --exec-mem SIZE              (default: \$EXEC_MEM)
  --exec-ovh SIZE              (default: \$EXEC_OVH)
  --max-part-bytes SIZE        (default: \$MAX_PART_BYTES)
  --adaptive true|false        (default: \$ADAPTIVE)
  --vec-reader true|false      (default: \$VEC_READER)
  --pq-batch N                 parquet columnar batch size (default: \$PARQUET_BATCH)
  --session-tz ZONE            Spark SQL session timeZone (default: \$SESSION_TZ)
  --target-file-mb N           Iceberg advisory file size MB (default: \$TARGET_FILE_MB)

Misc:
  --config FILE                source env overrides from FILE
  -h, --help                   show help
EOF
}

# ========= Parse CLI args =========
LAYOUT_CLI=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)            INPUT="$2"; shift 2;;
    --warehouse)        WAREHOUSE="$2"; shift 2;;
    --namespace)        NAMESPACE="$2"; shift 2;;
    --base-name)        BASE_NAME="$2"; shift 2;;
    --layout)           LAYOUT_CLI="$2"; shift 2;;

    --partition-by)     PARTITION_BY="$2"; shift 2;;
    --range-cols)       RANGE_COLS="$2"; shift 2;;
    --layout-cols)      LAYOUT_COLS="$2"; shift 2;;

    --shuffle)          SPARK_SHUF="$2"; shift 2;;
    --driver-mem)       DRIVER_MEM="$2"; shift 2;;
    --exec-mem)         EXEC_MEM="$2"; shift 2;;
    --exec-ovh)         EXEC_OVH="$2"; shift 2;;
    --max-part-bytes)   MAX_PART_BYTES="$2"; shift 2;;
    --adaptive)         ADAPTIVE="$2"; shift 2;;
    --vec-reader)       VEC_READER="$2"; shift 2;;
    --pq-batch)         PARQUET_BATCH="$2"; shift 2;;
    --session-tz)       SESSION_TZ="$2"; shift 2;;
    --target-file-mb)   TARGET_FILE_MB="$2"; shift 2;;

    --config)           CONFIG_FILE="$2"; shift 2;;
    -h|--help)          usage; exit 0;;
    *) echo "Unknown option: $1"; usage; exit 2;;
  esac
done

# ========= Load optional config file (env overrides) =========
if [[ -n "$CONFIG_FILE" ]]; then
  # Allows you to keep a preset of env vars in a .env-like file
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
fi

# ========= Helpers: normalize column lists to arrays =========
_split_cols() {
  local s="${1//,/ }"           # treat commas as spaces
  # shellcheck disable=SC2206
  COL_ARR=($s)                  # populate COL_ARR
}
_split_cols "$PARTITION_BY"; PARTITION_ARR=("${COL_ARR[@]}")
_split_cols "$RANGE_COLS";  RANGE_ARR=("${COL_ARR[@]}")
_split_cols "$LAYOUT_COLS"; LAYOUT_ARR=("${COL_ARR[@]}")

# ===== Normalize layout selection (CLI has priority over env LAYOUTS) =====
LAYOUT_RAW="${LAYOUT_CLI:-${LAYOUTS:-}}"
LAYOUT_ONE="${LAYOUT_RAW%%,*}"             # keep only the first comma-separated token
LAYOUT_ONE="$(echo "$LAYOUT_ONE" | awk '{print $1}')"  # keep only the first space-separated token
LAYOUT_ONE="${LAYOUT_ONE,,}"               # lowercase

# ========= Locate Spark (SPARK_HOME or spark-submit on PATH) =========
if [[ -z "${SPARK_HOME:-}" ]]; then
  if command -v spark-submit >/dev/null 2>&1; then
    export SPARK_HOME="$(dirname "$(dirname "$(command -v spark-submit)")")"
  else
    echo "spark-submit not found. Set SPARK_HOME." >&2; exit 1
  fi
fi

# Ensure warehouse directory exists
mkdir -p "$WAREHOUSE"

# Resolve warehouse to absolute path (robust for Spark configs)
WAREHOUSE_ABS="$(python3 - <<'PY'
import os; print(os.path.abspath(os.environ["WAREHOUSE"]))
PY
)"

# ========= Common Spark submit arguments (Iceberg + stability/resource knobs) =========
COMMON_SUBMIT_ARGS=(
  --packages "$ICEBERG_PKG"
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.local.type=hadoop
  --conf "spark.sql.catalog.local.warehouse=${WAREHOUSE_ABS}"
  --conf "spark.sql.session.timeZone=${SESSION_TZ}"

  # Resource & stability settings (helpful for large Parquet files)
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUF}"
  --conf "spark.driver.memory=${DRIVER_MEM}"
  --conf "spark.executor.memory=${EXEC_MEM}"
  --conf "spark.executor.memoryOverhead=${EXEC_OVH}"
  --conf "spark.sql.files.maxPartitionBytes=${MAX_PART_BYTES}"
  --conf "spark.sql.adaptive.enabled=${ADAPTIVE}"
  --conf "spark.sql.parquet.enableVectorizedReader=${VEC_READER}"
  --conf "spark.sql.parquet.columnarReaderBatchSize=${PARQUET_BATCH}"
)

# run_job <layout> <suffix>
# - layout: baseline|linear|zorder (forwarded to your Python writer)
# - suffix: used to construct the final table identifier: <namespace>.<base>_<suffix>
run_job() {
  local layout="$1"
  local suffix="$2"
  local table_id="${NAMESPACE}.${BASE_NAME}_${suffix}"

  echo "=== Iceberg write: ${table_id} (layout=${layout}) ==="

  # Build Python job arguments dynamically from normalized arrays
  local -a job_args=(
    --input "$INPUT"
    --warehouse "$WAREHOUSE_ABS"
    --table-identifier "$table_id"
    --mode overwrite
    --layout "$layout"
    --target-file-mb "$TARGET_FILE_MB"
  )
  if [[ ${#PARTITION_ARR[@]} -gt 0 ]]; then job_args+=(--partition-by "${PARTITION_ARR[@]}"); fi
  if [[ ${#RANGE_ARR[@]} -gt 0 ]];     then job_args+=(--range-cols    "${RANGE_ARR[@]}");    fi
  if [[ ${#LAYOUT_ARR[@]} -gt 0 ]];    then job_args+=(--layout-cols   "${LAYOUT_ARR[@]}");   fi

  # Submit the actual writer (expects ./lakehouse_op/iceberg_write_layout.py to parse these flags)
  "$SPARK_HOME/bin/spark-submit" \
    "${COMMON_SUBMIT_ARGS[@]}" \
    ./lakehouse_op/iceberg_write_layout.py \
      "${job_args[@]}"

  echo "[OK] $table_id"
}

# ========= Dispatch on the chosen layout (only one runs) =========
case "$LAYOUT_ONE" in
  "")
    echo "[INFO] No layout provided; nothing will run for Iceberg."
    ;;
  baseline)
    run_job "baseline" "baseline"
    ;;
  linear)
    # 'linear' typically implies pre- or post-write sorting by LAYOUT_COLS
    run_job "linear" "linear"
    ;;
  zorder|z-order)
    # 'zorder' relies on your Python writer to apply Z-ordering on LAYOUT_COLS
    run_job "zorder" "zorder"
    ;;
  *)
    echo "WARN: unknown Iceberg layout '$LAYOUT_ONE' (skipped)"
    ;;
esac
