#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash scripts/clean_data.sh                # interactive confirm
#   bash scripts/clean_data.sh --yes          # no prompt
#   bash scripts/clean_data.sh --scales "1 4" # only selected scales
#
# This script removes generated data under ./data/tpch_*/ for
# Delta (./delta), Hudi (./hudi), and Iceberg (./iceberg_wh).
# bash scripts/clean_data.sh --scales "4" --yes
ASSUME_YES=false
SCALES_FILTER=()

die() { echo "ERROR: $*" >&2; exit 1; }
safe_rm() {
  local p="$1"
  [[ "$p" == ./data/tpch_*/* || "$p" == ./data/tpch_* ]] || die "Refusing to delete outside ./data/tpch_* : $p"
  rm -rf "$p"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --yes|-y) ASSUME_YES=true; shift ;;
    --scales) read -r -a SCALES_FILTER <<< "$2"; shift 2 ;;
    -h|--help)
      sed -n '1,40p' "$0"; exit 0;;
    *) die "Unknown arg: $1" ;;
  esac
done

targets=()

if [[ ${#SCALES_FILTER[@]} -gt 0 ]]; then
  for s in "${SCALES_FILTER[@]}"; do
    targets+=("./data/tpch_${s}")
  done
else
  # default: clean all tpch_* under ./data
  while IFS= read -r -d '' d; do
    targets+=("$d")
  done < <(find ./data -maxdepth 1 -type d -name "tpch_*" -print0 || true)
fi

if [[ ${#targets[@]} -eq 0 ]]; then
  echo "[CLEAN] No ./data/tpch_* directories found. Nothing to do."
  exit 0
fi

echo "[CLEAN] The following directories will be removed:"
printf '  %s\n' "${targets[@]}"

if ! $ASSUME_YES; then
  read -r -p "Proceed? (y/N): " ans
  [[ "${ans:-}" == "y" || "${ans:-}" == "Y" ]] || { echo "Aborted."; exit 1; }
fi

for t in "${targets[@]}"; do
  echo "Removing: $t"
  safe_rm "$t"
done

echo "[CLEAN] Done."
