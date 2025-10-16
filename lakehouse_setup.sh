#!/usr/bin/env bash
# lakehouse_setup.sh — Validate or repair a Spark 3.5 + Delta/Iceberg/Hudi environment (user-space by default)
set -euo pipefail

# ---------------- CLI ----------------
MODE="validate"
USE_SUDO=0
CONFIG_FILE="${CONFIG_FILE:-./lakehouse.env}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--repair] [--use-sudo] [--config PATH]

  --repair       Attempt to fix issues (user-space Spark install; Java via package manager only if --use-sudo).
  --use-sudo     Allow sudo to install Java via apt/dnf/yum/brew. Without this, script never uses sudo.
  --config PATH  Path to environment file (default: ./lakehouse.env).
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repair) MODE="repair"; shift;;
    --use-sudo) USE_SUDO=1; shift;;
    --config) CONFIG_FILE="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 2;;
  esac
done

# ---------------- Config ----------------
if [[ -f "$CONFIG_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
else
  echo "Config file not found: $CONFIG_FILE" >&2
  exit 1
fi

# Defaults if not set by env file
SPARK_VERSION_EXPECTED="${SPARK_VERSION_EXPECTED:-3.5}"
SPARK_VERSION="${SPARK_VERSION:-3.5.1}"
SCALA_BIN="${SCALA_BIN:-2.12}"
HADOOP_PROFILE="${HADOOP_PROFILE:-hadoop3}"
FORCE_SPARK35="${FORCE_SPARK35:-1}"

JAVA_MIN_8_UPDATE="${JAVA_MIN_8_UPDATE:-371}"
JAVA_PREFERRED_MAJOR="${JAVA_PREFERRED_MAJOR:-17}"

ICEBERG_PKG="${ICEBERG_PKG:-org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0}"
HUDI_PKG="${HUDI_PKG:-org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2}"
DELTA_PKG="${DELTA_PKG:-io.delta:delta-spark_2.12:3.2.0}"

PREFIX="${PREFIX:-$HOME/.lakehouse}"
SPARK_TGZ="${SPARK_TGZ:-spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz}"
SPARK_DIR="${SPARK_DIR:-spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}}"
SPARK_HOME="${SPARK_HOME:-$PREFIX/$SPARK_DIR}"
ENV_FILE="$PREFIX/env"
BIN_DIR="$PREFIX/bin"

SPARK_DOWNLOAD_BASE="${SPARK_DOWNLOAD_BASE:-https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}}"

mkdir -p "$PREFIX" "$BIN_DIR"

# ---------------- Utils ----------------
ok()   { echo "OK: $*"; }
warn() { echo "WARN: $*" >&2; }
fail() { echo "FAIL: $*" >&2; exit 1; }

TIMEOUT_BIN=${TIMEOUT_BIN:-timeout}
if ! command -v "$TIMEOUT_BIN" >/dev/null 2>&1; then
  if command -v gtimeout >/dev/null 2>&1; then TIMEOUT_BIN=gtimeout; else TIMEOUT_BIN=true; fi
fi

have_cmd() { command -v "$1" >/dev/null 2>&1; }

# ---------------- Java ----------------
need_java_ok() {
  if ! have_cmd java; then return 1; fi
  local vline major
  vline=$(java -version 2>&1 | head -n1)
  if grep -q '"1\.8\.0_' <<<"$vline"; then
    local upd
    upd=$(java -version 2>&1 | sed -n 's/.*"1\.8\.0_\([0-9]\+\)".*/\1/p')
    [[ -n "$upd" && "$upd" -ge "$JAVA_MIN_8_UPDATE" ]] || return 1
    echo "$vline"; return 0
  else
    major=$(java -version 2>&1 | sed -n 's/.*"\([0-9]\+\)\..*/\1/p' | head -1)
    [[ "$major" =~ ^(11|17|21)$ ]] || return 1
    echo "$vline"; return 0
  fi
}

ensure_java() {
  local js
  js="$(need_java_ok || true)"
  if [[ -n "$js" ]]; then
    echo "Java: $js"; ok "Java looks good"; return 0
  fi
  if [[ "$MODE" != "repair" ]]; then
    fail "Java not suitable. Rerun with --repair or install JDK ${JAVA_PREFERRED_MAJOR} and ensure it is on PATH."
  fi
  if [[ "$USE_SUDO" -eq 1 ]]; then
    if have_cmd brew; then brew install openjdk@"$JAVA_PREFERRED_MAJOR" || true
    elif have_cmd apt-get; then sudo apt-get update -y && sudo apt-get install -y "openjdk-${JAVA_PREFERRED_MAJOR}-jdk" || true
    elif have_cmd dnf; then sudo dnf install -y "java-${JAVA_PREFERRED_MAJOR}-openjdk" || true
    elif have_cmd yum; then sudo yum install -y "java-${JAVA_PREFERRED_MAJOR}-openjdk" || true
    else warn "No known package manager. Please install JDK ${JAVA_PREFERRED_MAJOR} manually."
    fi
  else
    warn "Not using sudo. Please ensure a suitable JDK is available on PATH."
  fi
  js="$(need_java_ok || true)"
  [[ -n "$js" ]] || fail "Java still not suitable after repair attempt."
  echo "Java: $js"; ok "Java ready"
}

# ---------------- Spark ----------------
resolve_spark_ver() {
  local sv=""
  if have_cmd python3; then
    sv=$(python3 - <<'PY' 2>/dev/null || true
try:
  import pyspark; print(pyspark.__version__)
except Exception:
  pass
PY
) || true
  fi
  if [[ -z "$sv" && -f "$SPARK_HOME/RELEASE" ]]; then
    sv=$(grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' "$SPARK_HOME/RELEASE" | head -1 || true)
  fi
  if [[ -z "$sv" ]] && have_cmd spark-submit; then
    sv=$($TIMEOUT_BIN 5s bash -lc 'spark-submit --version 2>&1 | grep -Eo "[0-9]+\.[0-9]+\.[0-9]+"' | head -1 || true)
  fi
  echo "$sv"
}

download_spark_user() {
  local url="$SPARK_DOWNLOAD_BASE/$SPARK_TGZ"
  echo "Downloading Spark from: $url"
  if have_cmd curl; then
    curl -fL --retry 3 -o "$PREFIX/$SPARK_TGZ" "$url"
  elif have_cmd wget; then
    wget -O "$PREFIX/$SPARK_TGZ" "$url"
  else
    fail "Neither curl nor wget is available to download Spark."
  fi
  tar -xzf "$PREFIX/$SPARK_TGZ" -C "$PREFIX"
  ln -snf "$PREFIX/$SPARK_DIR" "$PREFIX/spark-current"
  ok "Spark extracted to $PREFIX/$SPARK_DIR"
}

ensure_spark() {
  echo "SPARK_HOME: ${SPARK_HOME:-<unset>}"
  if [[ ! -x "$SPARK_HOME/bin/spark-sql" ]]; then
    if [[ "$MODE" == "repair" ]]; then
      download_spark_user
      export SPARK_HOME="$PREFIX/$SPARK_DIR"
      export PATH="$SPARK_HOME/bin:$PATH"
    else
      fail "spark-sql not found at $SPARK_HOME/bin/spark-sql. Rerun with --repair to install Spark user-space."
    fi
  else
    export PATH="$SPARK_HOME/bin:$PATH"
  fi

  local sv
  sv="$(resolve_spark_ver || true)"
  echo "Spark: ${sv:-<unknown>}"
  if [[ -n "$sv" && "$FORCE_SPARK35" == "1" && "$sv" != 3.5.* ]]; then
    if [[ "$MODE" == "repair" ]]; then
      warn "Detected Spark $sv, target is 3.5.x. Reinstalling Spark ${SPARK_VERSION}..."
      download_spark_user
      sv="$(resolve_spark_ver || true)"
      echo "Spark: ${sv:-<unknown>}"
      [[ "$sv" == 3.5.* ]] || fail "Spark still not 3.5.x after repair."
    else
      fail "Spark $sv detected; need 3.5.x. Rerun with --repair."
    fi
  fi
}

# ---------------- Persist runtime env (BEFORE smoke test) ----------------
persist_env_runtime() {
  mkdir -p "$PREFIX" "$PREFIX/tmp" "$PREFIX/warehouse" "$PREFIX/derby"

  # Safe defaults for headless/SSH servers
  local SAFE_LOCAL_IP="127.0.0.1"
  local SAFE_HOST="localhost"

  cat > "$ENV_FILE" <<EOF
# Auto-generated Lakehouse runtime env
export SPARK_HOME="$SPARK_HOME"
export PATH="\$SPARK_HOME/bin:\$PATH"

# Safe local bindings and dirs
export SPARK_LOCAL_IP=${SPARK_LOCAL_IP:-$SAFE_LOCAL_IP}
export SPARK_LOCAL_HOSTNAME=${SPARK_LOCAL_HOSTNAME:-$SAFE_HOST}
export SPARK_DRIVER_BIND_ADDRESS=${SPARK_DRIVER_BIND_ADDRESS:-$SAFE_LOCAL_IP}
export SPARK_LOCAL_DIRS="${SPARK_LOCAL_DIRS:-$PREFIX/tmp}"
export _JAVA_OPTIONS="-Djava.net.preferIPv4Stack=true -Dderby.system.home=$PREFIX/derby \${_JAVA_OPTIONS:-}"

# Package coordinates
export ICEBERG_PKG="${ICEBERG_PKG}"
export HUDI_PKG="${HUDI_PKG}"
export DELTA_PKG="${DELTA_PKG}"

# Default Spark SQL conf to use a user-writable warehouse
export SPARK_SQL_DEFAULT_CONF='--conf spark.sql.warehouse.dir=$PREFIX/warehouse'
EOF

  ok "Wrote runtime env: $ENV_FILE"
  # Load it into current shell so smoke test uses it
  # shellcheck disable=SC1090
  source "$ENV_FILE"
}

# ---------------- Smoke test with logging & Hive-less default ----------------
smoke_test() {
  local LOG_FILE="$PREFIX/validate.log"
  echo "[spark-sql smoke] $(date)" >> "$LOG_FILE"

  # First try: in-memory catalog + writable warehouse
  if SPARK_PRINT_LAUNCH_COMMAND=1 \
     $TIMEOUT_BIN 25s bash -lc '"$SPARK_HOME/bin/spark-sql" \
        --conf spark.sql.catalogImplementation=in-memory \
        --conf spark.sql.warehouse.dir="'"$PREFIX"'/warehouse" \
        -e "select 1" >>"'"$LOG_FILE"'" 2>&1'; then
    ok "spark-sql smoke test passed (in-memory catalog)"
    return 0
  fi

  warn "spark-sql initial run failed; see $LOG_FILE"
  # Fallback already persisted in ENV_FILE; try a second time explicitly
  if SPARK_PRINT_LAUNCH_COMMAND=1 \
     $TIMEOUT_BIN 25s bash -lc '"$SPARK_HOME/bin/spark-sql" \
        --conf spark.sql.warehouse.dir="'"$PREFIX"'/warehouse" \
        -e "select 1" >>"'"$LOG_FILE"'" 2>&1'; then
    ok "spark-sql smoke test passed (hive catalog with safe warehouse)"
    return 0
  fi

  fail "spark-sql failed; check $LOG_FILE for details."
}

# ---------------- Package coords (informational) ----------------
show_package_coords() {
  echo "---- Package coordinates (no resolution attempted) ----"
  echo "Iceberg: $ICEBERG_PKG"
  echo "Hudi   : $HUDI_PKG"
  echo "Delta  : $DELTA_PKG"
}

# ---------------- Main ----------------
echo "=== Lakehouse environment: $MODE ==="
ensure_java
ensure_spark
persist_env_runtime
smoke_test
show_package_coords

# Create launcher (uses packages; optional)
cat > "$BIN_DIR/spark-sql-lakehouse" <<'EOS'
#!/usr/bin/env bash
: "${ICEBERG_PKG:?unset}"
: "${HUDI_PKG:?unset}"
: "${DELTA_PKG:?unset}"
exec spark-sql --packages "$ICEBERG_PKG","$HUDI_PKG","$DELTA_PKG" ${SPARK_SQL_DEFAULT_CONF:-} "$@"
EOS
chmod +x "$BIN_DIR/spark-sql-lakehouse"
ok "Launcher created: $BIN_DIR/spark-sql-lakehouse"

echo "---- Summary ----"
echo "PREFIX     : $PREFIX"
echo "SPARK_HOME : $SPARK_HOME"
echo "ENV_FILE   : $ENV_FILE"
echo "BIN_DIR    : $BIN_DIR"
echo "Log        : $PREFIX/validate.log"
echo
echo "Next steps:"
echo "  source \"$ENV_FILE\""
echo "  spark-sql -e 'select 42'     # or: ~/.lakehouse/bin/spark-sql-lakehouse -e 'select 42'"
