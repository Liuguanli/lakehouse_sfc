#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Build Delta/Hudi/Iceberg tables for the tpch_all dataset
# (fully covering all TPCH tables under /datasets/tpch_1/data by default).
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEFAULT_SPARK="${SPARK_HOME:-}"

usage() {
  cat <<'EOF'
tpch_all/load_data.sh
  --source DIR          Source directory with TPCH CSVs (default: /datasets/tpch_1/data)
  --data-root DIR       Output root for lakehouse tables (default: ./data/tpch_all)
  --engines LIST        Comma list of engines (delta,hudi,iceberg). Default: all three.
  --tables LIST         Comma list of tables (default: all TPCH tables)
  --overwrite           Overwrite existing outputs.
  --spark-home DIR      Override SPARK_HOME if not exported.
  --iceberg-catalog C   Iceberg catalog name (default: tpchall)
  --iceberg-namespace N Iceberg namespace/database (default: tpch_all)
  --iceberg-warehouse D Iceberg warehouse dir (default: ./data/tpch_all/iceberg_wh)
  -h | --help           Show this help.
EOF
}

SOURCE="/datasets/tpch_1/data"
DATA_ROOT="./data/tpch_all"
ENGINES="delta,hudi,iceberg"
TABLES="auto"
OVERWRITE=0
SPARK_HOME_OVERRIDE=""
ICEBERG_CATALOG="tpchall"
ICEBERG_NAMESPACE="tpch_all"
ICEBERG_WAREHOUSE="./data/tpch_all/iceberg_wh"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source) SOURCE="$2"; shift 2;;
    --data-root) DATA_ROOT="$2"; shift 2;;
    --engines) ENGINES="$2"; shift 2;;
    --tables) TABLES="$2"; shift 2;;
    --overwrite) OVERWRITE=1; shift;;
    --spark-home) SPARK_HOME_OVERRIDE="$2"; shift 2;;
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
  SPARK_HOME="$DEFAULT_SPARK"
fi

if [[ -z "${SPARK_HOME:-}" ]]; then
  echo "SPARK_HOME is not set. Use --spark-home or export SPARK_HOME." >&2
  exit 1
fi

SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
[[ -x "$SPARK_SUBMIT" ]] || { echo "spark-submit not executable: $SPARK_SUBMIT" >&2; exit 1; }

PKGS="io.delta:delta-spark_2.12:3.2.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"

pushd "$REPO_ROOT" >/dev/null

TABLE_ARG="$TABLES"
if [[ "$TABLES" == "auto" ]]; then
  TABLE_ARG="$(python - <<'PY'
from tpch_all_schemas import TABLE_LIST
print(",".join(TABLE_LIST))
PY
)"
fi

EXTRA_OVERWRITE=()
[[ $OVERWRITE -eq 1 ]] && EXTRA_OVERWRITE=(--overwrite)

set -x
"$SPARK_SUBMIT" \
  --packages "$PKGS" \
  lakehouse_op/tpch_all_loader.py \
  --source "$SOURCE" \
  --data-root "$DATA_ROOT" \
  --engines "$ENGINES" \
  --tables "$TABLE_ARG" \
  --iceberg-catalog "$ICEBERG_CATALOG" \
  --iceberg-namespace "$ICEBERG_NAMESPACE" \
  --iceberg-warehouse "$ICEBERG_WAREHOUSE" \
  "${EXTRA_OVERWRITE[@]}"
set +x

popd >/dev/null

echo "[DONE] tpch_all data load complete."
