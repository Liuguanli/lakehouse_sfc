#!/usr/bin/env bash
set -euo pipefail

# RQ5 master runner: execute all RQ5 matrix scripts (Delta/Iceberg x TPCH/Amazon) sequentially.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SCRIPTS=(
  "scripts/run_RQ_5_matrix_delta_tpch.sh"
  "scripts/run_RQ_5_matrix_iceberg_tpch.sh"
  # "scripts/run_RQ_5_matrix_delta_amazon.sh"
  # "scripts/run_RQ_5_matrix_iceberg_amazon.sh"
)

for rel in "${SCRIPTS[@]}"; do
  script="${ROOT_DIR}/${rel}"
  if [[ ! -x "$script" ]]; then
    echo "[SKIP] Missing or non-executable: $script"
    continue
  fi
  echo "=============================="
  echo ">>> Running $rel"
  echo "=============================="
  bash "$script"
done

echo "[DONE] RQ5 master run complete."
