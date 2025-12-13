#!/usr/bin/env bash
set -euo pipefail
#
# Rename TPCH RQ3 spec files under workload_spec/tpch_rq3 by replacing
# the RQ1 prefix with RQ3, and update the rq tag inside the YAML.
#
# Usage:
#   bash scripts/rename_tpch_rq3_specs.sh
#

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPEC_DIR="${ROOT_DIR}/workload_spec/tpch_rq3"

shopt -s nullglob
for f in "${SPEC_DIR}"/spec_tpch_RQ1_*.yaml; do
  base="$(basename "$f")"
  new_base="${base/spec_tpch_RQ1_/spec_tpch_RQ3_}"
  new_path="${SPEC_DIR}/${new_base}"
  echo "Renaming: $base -> $new_base"
  # Update content rq tag and any embedded prefix references.
  sed -e 's/\brq:\s*RQ1\b/rq: RQ3/' \
      -e 's/spec_tpch_RQ1_/spec_tpch_RQ3_/g' \
      "$f" > "${new_path}.tmp"
  mv "${new_path}.tmp" "$new_path"
done
shopt -u nullglob

echo "[DONE] Rename pass complete."
