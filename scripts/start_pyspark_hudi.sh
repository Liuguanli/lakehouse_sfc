#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Load project/user runtime env if present.
[[ -f "${HOME}/.lakehouse/env" ]] && source "${HOME}/.lakehouse/env"
[[ -f "${ROOT_DIR}/lakehouse.env" ]] && source "${ROOT_DIR}/lakehouse.env"

if [[ -z "${SPARK_HOME:-}" ]]; then
  echo "ERROR: SPARK_HOME is not set. Expected ~/.lakehouse/env to define it." >&2
  exit 1
fi

if [[ ! -x "${SPARK_HOME}/bin/pyspark" ]]; then
  echo "ERROR: ${SPARK_HOME}/bin/pyspark not found or not executable." >&2
  exit 1
fi

if [[ ! -x "${SPARK_HOME}/bin/spark-submit" ]]; then
  echo "ERROR: ${SPARK_HOME}/bin/spark-submit not found or not executable." >&2
  exit 1
fi

if [[ -z "${HUDI_PKG:-}" ]]; then
  echo "ERROR: HUDI_PKG is empty. Check lakehouse.env." >&2
  exit 1
fi

# If caller already provides --packages, do not override it.
has_packages=0
has_catalog_conf=0
has_ivy_conf=0
for arg in "$@"; do
  if [[ "$arg" == "--packages" ]]; then
    has_packages=1
  fi
  if [[ "$arg" == "spark.sql.catalogImplementation=in-memory" ]] || [[ "$arg" == "spark.sql.catalogImplementation=hive" ]]; then
    has_catalog_conf=1
  fi
  if [[ "$arg" == spark.jars.ivy=* ]]; then
    has_ivy_conf=1
  fi
done

EXTRA_ARGS=()

# Default to in-memory catalog to avoid Derby metastore lock conflicts.
if [[ "$has_catalog_conf" -eq 0 ]]; then
  EXTRA_ARGS+=(--conf "spark.sql.catalogImplementation=in-memory")
fi

# Ensure a writable warehouse directory even when user doesn't pass one.
EXTRA_ARGS+=(--conf "spark.sql.warehouse.dir=${PREFIX:-$HOME/.lakehouse}/warehouse")

# Keep Ivy cache in project workspace to avoid permission issues under restricted environments.
if [[ "$has_ivy_conf" -eq 0 ]]; then
  mkdir -p "${ROOT_DIR}/.ivy2/cache" "${ROOT_DIR}/.ivy2/jars"
  EXTRA_ARGS+=(--conf "spark.jars.ivy=${ROOT_DIR}/.ivy2")
fi

# If args include a Python script, use spark-submit (pyspark no longer supports python app args).
is_python_app=0
for arg in "$@"; do
  if [[ "$arg" == *.py ]]; then
    is_python_app=1
    break
  fi
done

launcher="${SPARK_HOME}/bin/pyspark"
if [[ "$is_python_app" -eq 1 ]]; then
  launcher="${SPARK_HOME}/bin/spark-submit"
  if [[ -n "${PYSPARK_SUBMIT_ARGS:-}" ]]; then
    echo "WARN: PYSPARK_SUBMIT_ARGS is ignored when running python scripts via spark-submit." >&2
    echo "      Pass --conf/--packages directly to this wrapper command instead." >&2
  fi
fi

echo "INFO: launcher=$(basename "$launcher")" >&2

if [[ "$has_packages" -eq 1 ]]; then
  exec "$launcher" "${EXTRA_ARGS[@]}" "$@"
fi

exec "$launcher" --packages "${HUDI_PKG}" "${EXTRA_ARGS[@]}" "$@"
