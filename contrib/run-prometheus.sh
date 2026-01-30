#!/usr/bin/env bash
set -eux -o pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PROM_VERSION="${PROM_VERSION:-3.9.1}"
PROM_CACHE_DIR="${PROM_CACHE_DIR:-${SCRIPT_DIR}/../opt/prometheus}" # from git root, ./opt/prometheus
PROM_DATA_DIR="${PROM_DATA_DIR:-${HOME}/.firedancer/prometheus/data}"
PROM_PASSWORD_FILE="${SCRIPT_DIR}/../opt/prometheus_remote_write_password.txt"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "${ARCH}" in
  x86_64|amd64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *) echo "unsupported arch: ${ARCH}" >&2; exit 1 ;;
esac

DOWNLOAD_URL="https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/prometheus-${PROM_VERSION}.${OS}-${ARCH}.tar.gz"

mkdir -p "${PROM_CACHE_DIR}" "${PROM_DATA_DIR}"

# create password file
if [[ ! -f $PROM_PASSWORD_FILE ]]; then
  read -r -s -p "Prometheus remote_write password: " PROMETHEUS_METRICS_PASSWORD
  echo "$PROMETHEUS_METRICS_PASSWORD" > "$PROM_PASSWORD_FILE"
  chmod 600 "$PROM_PASSWORD_FILE"
fi

# download prom if not already downloaded
if [[ ! -x "${PROM_CACHE_DIR}/prometheus-${PROM_VERSION}.${OS}-${ARCH}/prometheus" ]]; then
  wget -O - "${DOWNLOAD_URL}" | tar -xzf - -C "${PROM_CACHE_DIR}"
fi

exec "${PROM_CACHE_DIR}/prometheus-${PROM_VERSION}.${OS}-${ARCH}/prometheus" \
  --enable-feature=expand-env \
  --config.file="${SCRIPT_DIR}/prometheus.yml" \
  --storage.tsdb.path="${PROM_DATA_DIR}"
