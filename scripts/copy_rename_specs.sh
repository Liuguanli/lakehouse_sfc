#!/usr/bin/env bash
set -euo pipefail
#
# Copy specs from a source dir to a destination dir and rewrite the filename
# prefix + in-file references (e.g., rq label and spec prefixes).
#
# Usage examples:
#   # Copy TPCH RQ1 specs to tpch_rq5 and rename to RQ5
#   bash scripts/copy_rename_specs.sh \
#     --src workload_spec/tpch_rq1 \
#     --dst workload_spec/tpch_rq5 \
#     --old-prefix spec_tpch_RQ1_ \
#     --new-prefix spec_tpch_RQ5_ \
#     --old-rq RQ1 --new-rq RQ5
#
#   # Copy Amazon RQ1 specs to amazon_rq5 and rename to RQ5
#   bash scripts/copy_rename_specs.sh \
#     --src workload_spec/amazon_rq1 \
#     --dst workload_spec/amazon_rq5 \
#     --old-prefix spec_amazon_RQ1_ \
#     --new-prefix spec_amazon_RQ5_ \
#     --old-rq RQ1 --new-rq RQ5

SRC=""
DST=""
OLD_PREFIX=""
GLOB_PATTERN=""
NEW_PREFIX=""
OLD_RQ=""
NEW_RQ=""

usage() {
  cat <<'EOF'
copy_rename_specs.sh --src DIR --dst DIR --old-prefix STR --new-prefix STR [--old-rq RQ --new-rq RQ]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --src) SRC="$2"; shift 2;;
    --dst) DST="$2"; shift 2;;
    --old-prefix) OLD_PREFIX="$2"; shift 2;;
    --glob) GLOB_PATTERN="$2"; shift 2;;
    --new-prefix) NEW_PREFIX="$2"; shift 2;;
    --old-rq) OLD_RQ="$2"; shift 2;;
    --new-rq) NEW_RQ="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

[[ -d "$SRC" && -n "$DST" && -n "$OLD_PREFIX" && -n "$NEW_PREFIX" ]] || { usage; exit 2; }
mkdir -p "$DST"

pattern="${GLOB_PATTERN:-${OLD_PREFIX}*.yaml}"

shopt -s nullglob
for f in ${SRC}/${pattern}; do
  base="$(basename "$f")"
  new_base="${base/${OLD_PREFIX}/${NEW_PREFIX}}"
  out="${DST}/${new_base}"
  echo "Copying $base -> ${out#$PWD/}"
  tmp="${out}.tmp"
  cp "$f" "$tmp"
  if [[ -n "$OLD_RQ" && -n "$NEW_RQ" ]]; then
    sed -i -e "s/\brq:\s*${OLD_RQ}\b/rq: ${NEW_RQ}/g" "$tmp"
  fi
  sed -i -e "s/${OLD_PREFIX}/${NEW_PREFIX}/g" "$tmp"
  mv "$tmp" "$out"
done
shopt -u nullglob

echo "[DONE] Copied specs from ${SRC} to ${DST} with prefix ${NEW_PREFIX}"
