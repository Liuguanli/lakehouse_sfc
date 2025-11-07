#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Execute DBGEN TPCH streams against tpch_all datasets for
# Delta, Hudi, and Iceberg engines without touching legacy logic.
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEFAULT_STREAMS_ROOT="/datasets/tpch_1/workload"
DEFAULT_DATA_ROOT="./data/tpch_all"
DEFAULT_RESULTS_ROOT="results/tpch_all"

usage() {
  cat <<'EOF'
tpch_all/run_queries.sh
  --engines LIST        Comma-separated engines (delta,hudi,iceberg). Default: all three.
  --streams LIST        Comma-separated stream ids (stream_1,...). Default: auto-detect.
  --streams-root DIR    Directory containing stream_* folders (default: /datasets/tpch_1/workload).
  --data-root DIR       Root with materialised tables (default: ./data/tpch_all).
  --results-root DIR    Where to store CSV outputs (default: results/tpch_all).
  --spark-home DIR      Override SPARK_HOME.
  --action ACTION       Spark action (count|collect|show). Default: count.
  --timestamp TS        Override timestamp suffix.
  --iceberg-catalog C   Iceberg catalog name (default: tpchall).
  --iceberg-namespace N Iceberg namespace (default: tpch_all).
  --iceberg-warehouse D Iceberg warehouse (default: ./data/tpch_all/iceberg_wh).
  -h | --help           Show help.
EOF
}

ENGINES="delta,hudi,iceberg"
STREAMS="auto"
STREAMS_ROOT="$DEFAULT_STREAMS_ROOT"
DATA_ROOT="$DEFAULT_DATA_ROOT"
RESULTS_ROOT="$DEFAULT_RESULTS_ROOT"
SPARK_HOME_OVERRIDE=""
ACTION="count"
TIMESTAMP=""
ICEBERG_CATALOG="tpchall"
ICEBERG_NAMESPACE="tpch_all"
ICEBERG_WAREHOUSE="./data/tpch_all/iceberg_wh"
DRIVER_MEM="${TPCH_ALL_DRIVER_MEM:-96g}"
EXEC_MEM="${TPCH_ALL_EXEC_MEM:-96g}"
EXEC_OVH="${TPCH_ALL_EXEC_OVH:-32g}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --engines) ENGINES="$2"; shift 2;;
    --streams) STREAMS="$2"; shift 2;;
    --streams-root) STREAMS_ROOT="$2"; shift 2;;
    --data-root) DATA_ROOT="$2"; shift 2;;
    --results-root) RESULTS_ROOT="$2"; shift 2;;
    --spark-home) SPARK_HOME_OVERRIDE="$2"; shift 2;;
    --action) ACTION="$2"; shift 2;;
    --timestamp) TIMESTAMP="$2"; shift 2;;
    --iceberg-catalog) ICEBERG_CATALOG="$2"; shift 2;;
    --iceberg-namespace) ICEBERG_NAMESPACE="$2"; shift 2;;
    --iceberg-warehouse) ICEBERG_WAREHOUSE="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown option: $1"; usage; exit 2;;
  esac
done

if [[ -n "$SPARK_HOME_OVERRIDE" ]]; then
  SPARK_HOME="$SPARK_HOME_OVERRIDE"
elif [[ -z "${SPARK_HOME:-}" ]]; then
  echo "SPARK_HOME is not set. Use --spark-home or export SPARK_HOME." >&2
  exit 1
fi

SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
[[ -x "$SPARK_SUBMIT" ]] || { echo "spark-submit not executable: $SPARK_SUBMIT" >&2; exit 1; }

PKGS="io.delta:delta-spark_2.12:3.2.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"

pushd "$REPO_ROOT" >/dev/null

EXTRA_TS=()
if [[ -n "$TIMESTAMP" ]]; then
  EXTRA_TS=(--timestamp "$TIMESTAMP")
fi

for eng in ${ENGINES//,/ }; do
  eng_lc="$(echo "$eng" | tr '[:upper:]' '[:lower:]')"
  echo ">>> Running tpch_all queries for engine=${eng_lc}"
  set -x
  PYTHONPATH="$REPO_ROOT" "$SPARK_SUBMIT" \
    --driver-memory "$DRIVER_MEM" \
    --executor-memory "$EXEC_MEM" \
    --conf "spark.executor.memoryOverhead=$EXEC_OVH" \
    --packages "$PKGS" \
    lakehouse_op/tpch_all_runner.py \
      --engine "$eng_lc" \
      --data-root "$DATA_ROOT" \
      --streams-root "$STREAMS_ROOT" \
      --streams "$STREAMS" \
      --results-root "$RESULTS_ROOT" \
      --action "$ACTION" \
      --iceberg-catalog "$ICEBERG_CATALOG" \
      --iceberg-namespace "$ICEBERG_NAMESPACE" \
      --iceberg-warehouse "$ICEBERG_WAREHOUSE" \
      "${EXTRA_TS[@]}"
  set +x
done

popd >/dev/null

echo "[DONE] tpch_all query execution complete. Results under ${RESULTS_ROOT}."
