#!/usr/bin/env bash
set -eux -o pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PROM_VERSION="${PROM_VERSION:-3.9.1}"
PROM_CACHE_DIR="${PROM_CACHE_DIR:-${SCRIPT_DIR}/../opt/prometheus}" # from git root, ./opt/prometheus
PROM_DATA_DIR="${PROM_DATA_DIR:-${HOME}/.firedancer/prometheus/data}"
PROM_PASSWORD_FILE="${PROM_PASSWORD_FILE:-${SCRIPT_DIR}/../opt/prometheus_remote_write_password.txt}"
PROM_SCRAPE_INTERVAL="${PROM_SCRAPE_INTERVAL:-1s}"
PROM_SYSTEMD_SERVICE="${PROM_SYSTEMD_SERVICE:-firedancer-prometheus.service}"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "${ARCH}" in
  x86_64|amd64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *) echo "unsupported arch: ${ARCH}" >&2; exit 1 ;;
esac

USAGE='Usage: run-prometheus.sh [run|install] [--user|--system]

Commands:
  run       Run Prometheus with the contrib config (default)
  install   Create a systemd service for Prometheus

Options (install only):
  --user     Install a user service in ~/.config/systemd/user (default)
  --system   Install a system service in /etc/systemd/system (requires root)
'

run_prometheus() {
  shift
  export PROM_SCRAPE_INTERVAL
  mkdir -p "${PROM_CACHE_DIR}" "${PROM_DATA_DIR}" "$(dirname "${PROM_PASSWORD_FILE}")"
  if [[ ! -f "${PROM_PASSWORD_FILE}" ]]; then
    read -r -s -p "Prometheus remote_write password: " PROMETHEUS_METRICS_PASSWORD
    echo
    printf "%s" "${PROMETHEUS_METRICS_PASSWORD}" > "${PROM_PASSWORD_FILE}"
    chmod 600 "${PROM_PASSWORD_FILE}"
  fi
  if [[ ! -x "${PROM_CACHE_DIR}/prometheus-${PROM_VERSION}.${OS}-${ARCH}/prometheus" ]]; then
    wget -O - "https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/prometheus-${PROM_VERSION}.${OS}-${ARCH}.tar.gz" | tar -xzf - -C "${PROM_CACHE_DIR}"
  fi
  exec "${PROM_CACHE_DIR}/prometheus-${PROM_VERSION}.${OS}-${ARCH}/prometheus" \
    --enable-feature=expand-env \
    --config.file="${SCRIPT_DIR}/prometheus.yml" \
    --storage.tsdb.path="${PROM_DATA_DIR}"
}

install_systemd() {
  shift
  scope="user"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --user) scope="user" ;;
      --system) scope="system" ;;
      -h|--help) echo "${USAGE}"; exit 0 ;;
      *) echo "unknown option: $1" >&2; echo "${USAGE}" >&2; exit 1 ;;
    esac
    shift
  done

  if [[ "${scope}" == "system" && "${EUID}" -ne 0 ]]; then
    echo "system install requires root; re-run with sudo and --system" >&2
    exit 1
  fi

  if [[ "${scope}" == "system" ]]; then
    service_dir="/etc/systemd/system"
    systemctl_cmd=(systemctl)
    install_target="multi-user.target"
  else
    service_dir="${HOME}/.config/systemd/user"
    systemctl_cmd=(systemctl --user)
    install_target="default.target"
  fi

  mkdir -p "${PROM_CACHE_DIR}" "${PROM_DATA_DIR}" "$(dirname "${PROM_PASSWORD_FILE}")"
  if [[ ! -f "${PROM_PASSWORD_FILE}" ]]; then
    read -r -s -p "Prometheus remote_write password: " PROMETHEUS_METRICS_PASSWORD
    echo
    printf "%s" "${PROMETHEUS_METRICS_PASSWORD}" > "${PROM_PASSWORD_FILE}"
    chmod 600 "${PROM_PASSWORD_FILE}"
  fi
  mkdir -p "${service_dir}"

  cat > "${service_dir}/${PROM_SYSTEMD_SERVICE}" <<EOF
[Unit]
Description=Firedancer Prometheus
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
WorkingDirectory=${SCRIPT_DIR}
ExecStart=${SCRIPT_DIR}/run-prometheus.sh run
Restart=on-failure
RestartSec=3
Environment="PROM_VERSION=${PROM_VERSION}"
Environment="PROM_CACHE_DIR=${PROM_CACHE_DIR}"
Environment="PROM_DATA_DIR=${PROM_DATA_DIR}"
Environment="PROM_PASSWORD_FILE=${PROM_PASSWORD_FILE}"
Environment="PROM_SCRAPE_INTERVAL=${PROM_SCRAPE_INTERVAL}"

[Install]
WantedBy=${install_target}
EOF

  "${systemctl_cmd[@]}" daemon-reload
  "${systemctl_cmd[@]}" enable --now "${PROM_SYSTEMD_SERVICE}"
}

main() {
  case "${1:-run}" in
    run) run_prometheus "$@" ;;
    install) install_systemd "$@" ;;
    -h|--help|help)
      echo "${USAGE}"
      ;;
    *)
      echo "unknown command: $1" >&2
      echo "${USAGE}" >&2
      exit 1
      ;;
  esac
}

main "$@"
