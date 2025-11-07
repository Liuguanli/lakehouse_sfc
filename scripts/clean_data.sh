#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash scripts/clean_data.sh                # interactive confirm
#   bash scripts/clean_data.sh --yes          # no prompt (all tpch_* + optional amazon)
#   bash scripts/clean_data.sh --scales "1 4" # only selected TPCH scales
#   bash scripts/clean_data.sh --amazon       # clean ./data/amazon (tables for Amazon benchmark)
#
# This script removes generated data under ./data/tpch_*/ for
# Delta (./delta), Hudi (./hudi), and Iceberg (./iceberg_wh).
# bash scripts/clean_data.sh --scales "4" --yes
ASSUME_YES=false
SCALES_FILTER=()
CLEAN_AMAZON=false

die() { echo "ERROR: $*" >&2; exit 1; }
safe_rm() {
  local p="$1"
  case "$p" in
    ./data/tpch_*|./data/tpch_*/*|./data/amazon|./data/amazon/*) ;;
    *) die "Refusing to delete outside ./data/tpch_* or ./data/amazon : $p" ;;
  esac
  rm -rf "$p"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --yes|-y) ASSUME_YES=true; shift ;;
    --scales) read -r -a SCALES_FILTER <<< "$2"; shift 2 ;;
    --amazon) CLEAN_AMAZON=true; shift ;;
    -h|--help)
      sed -n '1,40p' "$0"; exit 0;;
    *) die "Unknown arg: $1" ;;
  esac
done

targets=()

if [[ ${#SCALES_FILTER[@]} -gt 0 ]]; then
  tmp_scales=()
  for s in "${SCALES_FILTER[@]}"; do
    if [[ "${s,,}" == "amazon" ]]; then
      CLEAN_AMAZON=true
    else
      tmp_scales+=("$s")
    fi
  done
  SCALES_FILTER=("${tmp_scales[@]}")

  for s in "${SCALES_FILTER[@]}"; do
    targets+=("./data/tpch_${s}")
  done
else
  # default: clean all tpch_* under ./data
  while IFS= read -r -d '' d; do
    targets+=("$d")
  done < <(find ./data -maxdepth 1 -type d -name "tpch_*" -print0 || true)
  $CLEAN_AMAZON && targets+=("./data/amazon")
fi

$CLEAN_AMAZON && [[ ${#SCALES_FILTER[@]} -gt 0 ]] && targets+=("./data/amazon")

# Deduplicate targets
if [[ ${#targets[@]} -gt 0 ]]; then
  declare -A _seen=()
  uniq_targets=()
  for t in "${targets[@]}"; do
    [[ -z "${t}" ]] && continue
    if [[ -z "${_seen[$t]:-}" ]]; then
      uniq_targets+=("$t")
      _seen[$t]=1
    fi
  done
  targets=("${uniq_targets[@]}")
fi

if [[ ${#targets[@]} -eq 0 ]]; then
  echo "[CLEAN] No targets found. Nothing to do."
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
