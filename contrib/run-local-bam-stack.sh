#!/usr/bin/env bash

# Starts:
# 1. mock BAM server
# 2. Frankendancer with the local BAM config
# 3. a continuous fddev txn loop
#
# Assumes the local cluster is already running.

set -euo pipefail
IFS=$'\n\t'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)

CONFIG="${CONFIG_PATH:-${ROOT}/local.toml}"
if [[ ! -f "${CONFIG}" ]]; then
  CONFIG="${ROOT}/src/app/fdctl/config/local-bam.toml"
fi

FDCTL="${ROOT}/build/native/gcc/bin/fdctl"
FDDEV="${ROOT}/build/native/gcc/bin/fddev"
BAM_MANIFEST="${ROOT}/contrib/bam-test-server/Cargo.toml"

LOG_DIR="${LOG_DIR:-$(mktemp -d /tmp/firebam-local-bam.XXXXXX)}"
FD_MODE="${FD_MODE:-run}"      # run | dev
TXN_COUNT="${TXN_COUNT:-128}"  # fddev txn max is 128
TXN_SLEEP="${TXN_SLEEP:-0.2}"

die() {
  echo "error: $*" >&2
  exit 1
}

[[ -f "${CONFIG}" ]] || die "config not found"
[[ -x "${FDCTL}" ]] || die "missing ${FDCTL}; build fdctl first"
[[ -x "${FDDEV}" ]] || die "missing ${FDDEV}; build fddev first"
[[ -f "${BAM_MANIFEST}" ]] || die "missing ${BAM_MANIFEST}"
command -v cargo >/dev/null 2>&1 || die "cargo not found"

SUDO=()
if [[ "${USE_SUDO:-1}" != "0" && "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo not found"
  SUDO=( sudo )
fi

case "${FD_MODE}" in
  run)
    FD_CMD=( "${SUDO[@]}" "${FDCTL}" run --config "${CONFIG}" )
    ;;
  dev)
    FD_CMD=( "${SUDO[@]}" "${FDDEV}" dev --no-watch --no-configure --config "${CONFIG}" )
    ;;
  *)
    die "FD_MODE must be run or dev"
    ;;
esac

mkdir -p "${LOG_DIR}"
BAM_LOG="${LOG_DIR}/bam.log"
FD_LOG="${LOG_DIR}/fd.log"
TXN_LOG="${LOG_DIR}/txn.log"

bam_pid=""
fd_pid=""
txn_pid=""

cleanup() {
  trap - EXIT INT TERM
  set +e
  [[ -n "${txn_pid}" ]] && kill "${txn_pid}" 2>/dev/null || true
  [[ -n "${fd_pid}" ]] && kill "${fd_pid}" 2>/dev/null || true
  [[ -n "${bam_pid}" ]] && kill "${bam_pid}" 2>/dev/null || true
  wait "${txn_pid}" 2>/dev/null || true
  wait "${fd_pid}" 2>/dev/null || true
  wait "${bam_pid}" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

cargo run --manifest-path "${BAM_MANIFEST}" -- \
  --bind 127.0.0.1:50055 \
  --tpu-ip 127.0.0.1 \
  --tpu-port 9007 \
  --tpu-fwd-ip 127.0.0.1 \
  --tpu-fwd-port 9008 \
  >>"${BAM_LOG}" 2>&1 &
bam_pid=$!

"${FD_CMD[@]}" >>"${FD_LOG}" 2>&1 &
fd_pid=$!

(
  while true; do
    "${FDDEV}" txn --config "${CONFIG}" --count "${TXN_COUNT}"
    sleep "${TXN_SLEEP}"
  done
) >>"${TXN_LOG}" 2>&1 &
txn_pid=$!

cat <<EOF
mock BAM server pid: ${bam_pid}
Frankendancer pid:    ${fd_pid}
txn spammer pid:      ${txn_pid}

config: ${CONFIG}
log dir: ${LOG_DIR}

Ctrl-C stops everything.
EOF

wait -n "${bam_pid}" "${fd_pid}" "${txn_pid}"
