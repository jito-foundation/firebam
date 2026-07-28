#!/usr/bin/env bash

# Validates BAM-controller hard restart churn against a live local fddev node:
# 1. initialize the local dev config;
# 2. start the benign BAM controller scenario;
# 3. restart the BAM controller once the first scheduler stream is healthy;
# 4. confirm the validator performs a fresh BAM handshake after the controller restarts.

set -euo pipefail
IFS=$'
	'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)

CONFIG="${ROOT}/contrib/local-bam-compact.toml"
FD_BIN="${FD_BIN:-${FDDEV:-${ROOT}/build/native/gcc/bin/fddev}}"
CONTROL_BIN="${CONTROL_BIN:-${FDCTL:-${ROOT}/build/native/gcc/bin/fdctl}}"
TIMEOUT_SECS=120
USE_SUDO="${USE_SUDO:-1}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS] [-- extra validate-local-bam-live args]

Options:
  --config PATH        Validator config path (default: contrib/local-bam-compact.toml)
  --fd-bin PATH        Validator binary (default: build/native/gcc/bin/fddev)
  --control-bin PATH   Control binary for operator events (default: build/native/gcc/bin/fdctl)
  --timeout-secs N     Validation timeout in seconds (default: 120)
  -h, --help           Show this help
EOF
}

PASSTHRU=()
while (($#)); do
  case "$1" in
    --config)
      CONFIG="$2"
      shift 2
      ;;
    --fd-bin)
      FD_BIN="$2"
      shift 2
      ;;
    --control-bin)
      CONTROL_BIN="$2"
      shift 2
      ;;
    --timeout-secs)
      TIMEOUT_SECS="$2"
      shift 2
      ;;
    --)
      shift
      PASSTHRU+=( "$@" )
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      PASSTHRU+=( "$1" )
      shift
      ;;
  esac
done

die() {
  echo "error: $*" >&2
  exit 1
}

[[ -x "${FD_BIN}" ]] || die "missing validator binary ${FD_BIN}"
[[ -x "${CONTROL_BIN}" ]] || die "missing control binary ${CONTROL_BIN}"
[[ -f "${CONFIG}" ]] || die "missing config ${CONFIG}"

SUDO=()
if [[ "${USE_SUDO}" != "0" && "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo not found"
  SUDO=( sudo )
fi

"${SUDO[@]}" "${FD_BIN}" configure init all --config "${CONFIG}" >/dev/null 2>&1

exec "${ROOT}/contrib/validate-local-bam-live.sh" \
  --scenario benign \
  --operator-events "${ROOT}/contrib/bam-operator-events/bam-restart.txt" \
  --config "${CONFIG}" \
  --fd-bin "${FD_BIN}" \
  --control-bin "${CONTROL_BIN}" \
  --skip-configure \
  --timeout-secs "${TIMEOUT_SECS}" \
  --expect-bam-count '^GetAuthChallenge:' 2 \
  --expect-bam-count '^GetBuilderConfig:' 2 \
  --expect-bam-count '^InitSchedulerStream:' 2 \
  --expect-bam-count 'scheduler<-validator auth proof' 2 \
  --expect-fd-pattern 'Disconnected: recvmsg error' \
  --expect-operator-pattern 'operator: restart_bam stopping pid=' \
  --expect-operator-pattern 'operator: restart_bam started pid=' \
  "${PASSTHRU[@]}"
