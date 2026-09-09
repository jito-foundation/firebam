#!/usr/bin/env bash

# Validates a stronger live BAM scenario:
# 1. start local fddev with RPC enabled;
# 2. build two locally adapted transfers from the same funded source account;
# 3. place both transfers in one atomic BAM batch with revert_on_error=true;
# 4. force the second transfer to overdraw the payer;
# 5. confirm the validator reports not_committed and that no balances changed.

set -euo pipefail
IFS=$'\n\t'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)

CONFIG="${ROOT}/contrib/local-bam-compact.toml"
RPC_URL="${RPC_URL:-http://127.0.0.1:8899}"
BAM_BIND="${BAM_BIND:-127.0.0.1:50055}"
BAM_TPU_PORT="${BAM_TPU_PORT:-9007}"
BAM_TPU_FWD_PORT="${BAM_TPU_FWD_PORT:-9008}"
BAM_SHRED_PORT="${BAM_SHRED_PORT:-9009}"
LOG_DIR=""
PRE_SLEEP_MS=1000
POST_SLEEP_MS=3000
USE_SUDO="${USE_SUDO:-1}"
RESERVE_LAMPORTS=1000000000
OVERFLOW_LAMPORTS=1000000000

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --config PATH              fddev config path (default: contrib/local-bam-compact.toml)
  --rpc-url URL              JSON RPC URL (default: http://127.0.0.1:8899)
  --bam-bind HOST:PORT       BAM test-server bind address
  --bam-tpu-port PORT        TPU port advertised by BAM test server
  --bam-tpu-fwd-port PORT    TPU forward port advertised by BAM
  --bam-shred-port PORT      Shred port advertised by BAM
  --log-dir PATH             Artifact directory (default: mktemp)
  --pre-sleep-ms N           Scenario delay before sending the BAM batch
  --post-sleep-ms N          Scenario delay after sending the BAM batch
  --reserve-lamports N       Remaining lamports to leave after tx0 before fees
  --overflow-lamports N      Additional lamports that make tx1 exceed remaining balance
  -h, --help                 Show this help
EOF
}

while (($#)); do
  case "$1" in
    --config)
      CONFIG="$2"
      shift 2
      ;;
    --rpc-url)
      RPC_URL="$2"
      shift 2
      ;;
    --bam-bind)
      BAM_BIND="$2"
      shift 2
      ;;
    --bam-tpu-port)
      BAM_TPU_PORT="$2"
      shift 2
      ;;
    --bam-tpu-fwd-port)
      BAM_TPU_FWD_PORT="$2"
      shift 2
      ;;
    --bam-shred-port)
      BAM_SHRED_PORT="$2"
      shift 2
      ;;
    --log-dir)
      LOG_DIR="$2"
      shift 2
      ;;
    --pre-sleep-ms)
      PRE_SLEEP_MS="$2"
      shift 2
      ;;
    --post-sleep-ms)
      POST_SLEEP_MS="$2"
      shift 2
      ;;
    --reserve-lamports)
      RESERVE_LAMPORTS="$2"
      shift 2
      ;;
    --overflow-lamports)
      OVERFLOW_LAMPORTS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

FDDEV="${ROOT}/build/native/gcc/bin/fddev"
BRIDGE_MANIFEST="${ROOT}/contrib/txnctx-bridge/Cargo.toml"
BAM_MANIFEST="${ROOT}/contrib/bam-test-server/Cargo.toml"

die() {
  echo "error: $*" >&2
  exit 1
}

[[ -x "${FDDEV}" ]] || die "missing ${FDDEV}; build fddev first"
[[ -f "${BRIDGE_MANIFEST}" ]] || die "missing ${BRIDGE_MANIFEST}"
[[ -f "${BAM_MANIFEST}" ]] || die "missing ${BAM_MANIFEST}"
[[ -f "${CONFIG}" ]] || die "missing config ${CONFIG}"
command -v cargo >/dev/null 2>&1 || die "cargo not found"
command -v curl >/dev/null 2>&1 || die "curl not found"
command -v jq >/dev/null 2>&1 || die "jq not found"

SUDO=()
if [[ "${USE_SUDO}" != "0" && "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo not found"
  SUDO=( sudo )
fi

if [[ -z "${LOG_DIR}" ]]; then
  LOG_DIR=$(mktemp -d /tmp/firebam-live-atomic-revert.XXXXXX)
fi
mkdir -p "${LOG_DIR}"

FD_LOG="${LOG_DIR}/fd.log"
BAM_LOG="${LOG_DIR}/bam.log"
RPC_LOG="${LOG_DIR}/rpc.log"

cleanup() {
  trap - EXIT INT TERM
  set +e
  [[ -n "${BAM_PID:-}" ]] && kill "${BAM_PID}" 2>/dev/null || true
  [[ -n "${FD_PID:-}" ]] && "${SUDO[@]}" kill "${FD_PID}" 2>/dev/null || true
  [[ -n "${BAM_PID:-}" ]] && wait "${BAM_PID}" 2>/dev/null || true
  [[ -n "${FD_PID:-}" ]] && wait "${FD_PID}" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

rpc_latest_blockhash() {
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash","params":[{"commitment":"processed"}]}' \
    | tee -a "${RPC_LOG}" \
    | jq -e -r '.result.value.blockhash'
}

rpc_balance() {
  local pubkey="$1"
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBalance\",\"params\":[\"${pubkey}\",{\"commitment\":\"processed\"}]}" \
    | tee -a "${RPC_LOG}" \
    | jq -e -r '.result.value'
}

wait_for_pattern() {
  local file="$1"
  local pattern="$2"
  local deadline="$3"
  while :; do
    if grep -qE "${pattern}" "${file}" 2>/dev/null; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 1
  done
  return 1
}

extract_bridge_field() {
  local field="$1"
  local file="$2"
  local line
  line=$(grep '^adapted family=' "${file}" || true)
  [[ -n "${line}" ]] || die "bridge summary missing in ${file}"
  case "${field}" in
    from)
      echo "${line}" | sed -n 's/.* from=\([^ ]*\) to=.*/\1/p'
      ;;
    to)
      echo "${line}" | sed -n 's/.* to=\([^ ]*\) lamports=.*/\1/p'
      ;;
    lamports)
      echo "${line}" | sed -n 's/.* lamports=\([0-9][0-9]*\) .*/\1/p'
      ;;
    *)
      die "unsupported bridge field ${field}"
      ;;
  esac
}

echo "logs: ${LOG_DIR}"
echo "config: ${CONFIG}"
echo "rpc: ${RPC_URL}"

"${SUDO[@]}" "${FDDEV}" configure init all --config "${CONFIG}" >/dev/null 2>&1

"${SUDO[@]}" "${FDDEV}" dev \
  --no-watch \
  --no-configure \
  --config "${CONFIG}" \
  >"${FD_LOG}" 2>&1 &
FD_PID=$!

BLOCKHASH=""
for _ in $(seq 1 60); do
  if BLOCKHASH=$(rpc_latest_blockhash 2>/dev/null); then
    break
  fi
  sleep 1
done
[[ -n "${BLOCKHASH}" ]] || die "validator RPC did not become ready at ${RPC_URL}"

cargo run --quiet --manifest-path "${BRIDGE_MANIFEST}" --bin gen_simple_system_txnctx -- \
  --output "${LOG_DIR}/probe.txnctx" \
  --lamports 1 \
  >"${LOG_DIR}/probe-gen.out"

cargo run --quiet --manifest-path "${BRIDGE_MANIFEST}" --bin txnctx-bridge -- \
  --input "${LOG_DIR}/probe.txnctx" \
  --output "${LOG_DIR}/probe-recipient-1.txt" \
  --adapt-mode local-system-transfer \
  --recent-blockhash "${BLOCKHASH}" \
  --from-genesis-account 0 \
  --to-genesis-account 1 \
  >"${LOG_DIR}/probe-recipient-1.out"

cargo run --quiet --manifest-path "${BRIDGE_MANIFEST}" --bin txnctx-bridge -- \
  --input "${LOG_DIR}/probe.txnctx" \
  --output "${LOG_DIR}/probe-recipient-2.txt" \
  --adapt-mode local-system-transfer \
  --recent-blockhash "${BLOCKHASH}" \
  --from-genesis-account 0 \
  --to-genesis-account 2 \
  >"${LOG_DIR}/probe-recipient-2.out"

FROM=$(extract_bridge_field from "${LOG_DIR}/probe-recipient-1.out")
TO1=$(extract_bridge_field to "${LOG_DIR}/probe-recipient-1.out")
TO2=$(extract_bridge_field to "${LOG_DIR}/probe-recipient-2.out")

PRE_FROM=$(rpc_balance "${FROM}")
PRE_TO1=$(rpc_balance "${TO1}")
PRE_TO2=$(rpc_balance "${TO2}")

(( PRE_FROM > RESERVE_LAMPORTS + OVERFLOW_LAMPORTS + 1000000 )) || die "payer balance ${PRE_FROM} is too small for the configured revert scenario"

LAMPORTS_1=$((PRE_FROM - RESERVE_LAMPORTS))
LAMPORTS_2=$((RESERVE_LAMPORTS + OVERFLOW_LAMPORTS))

echo "blockhash: ${BLOCKHASH}"
echo "from: ${FROM} pre=${PRE_FROM}"
echo "to1:  ${TO1} pre=${PRE_TO1}"
echo "to2:  ${TO2} pre=${PRE_TO2}"
echo "lamports tx0: ${LAMPORTS_1}"
echo "lamports tx1: ${LAMPORTS_2}"

cargo run --quiet --manifest-path "${BRIDGE_MANIFEST}" --bin gen_simple_system_txnctx -- \
  --output "${LOG_DIR}/tx0.txnctx" \
  --lamports "${LAMPORTS_1}" \
  >"${LOG_DIR}/tx0-gen.out"

cargo run --quiet --manifest-path "${BRIDGE_MANIFEST}" --bin gen_simple_system_txnctx -- \
  --output "${LOG_DIR}/tx1.txnctx" \
  --lamports "${LAMPORTS_2}" \
  >"${LOG_DIR}/tx1-gen.out"

cargo run --quiet --manifest-path "${BRIDGE_MANIFEST}" --bin txnctx-bridge -- \
  --input "${LOG_DIR}/tx0.txnctx" \
  --output "${LOG_DIR}/tx0.txt" \
  --adapt-mode local-system-transfer \
  --recent-blockhash "${BLOCKHASH}" \
  --from-genesis-account 0 \
  --to-genesis-account 1 \
  >"${LOG_DIR}/tx0-bridge.out"

cargo run --quiet --manifest-path "${BRIDGE_MANIFEST}" --bin txnctx-bridge -- \
  --input "${LOG_DIR}/tx1.txnctx" \
  --output "${LOG_DIR}/tx1.txt" \
  --adapt-mode local-system-transfer \
  --recent-blockhash "${BLOCKHASH}" \
  --from-genesis-account 0 \
  --to-genesis-account 2 \
  >"${LOG_DIR}/tx1-bridge.out"

cat "${LOG_DIR}/tx0.txt" "${LOG_DIR}/tx1.txt" >"${LOG_DIR}/packets.txt"

cat >"${LOG_DIR}/scenario.toml" <<EOF
description = "Two locally adapted transfers from the same payer. The second overdraws the remaining balance, so the atomic batch should not commit."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = ${PRE_SLEEP_MS}

[[events]]
type = "send_batch"
seq_id = 1
packets_base64_file = "${LOG_DIR}/packets.txt"
max_schedule_slot = "max"
simple_vote_tx = false
revert_on_error = true

[[events]]
type = "sleep"
ms = ${POST_SLEEP_MS}
EOF

cargo run --quiet --manifest-path "${BAM_MANIFEST}" --bin bam-test-server -- \
  --scenario benign \
  --scenario-file "${LOG_DIR}/scenario.toml" \
  --bind "${BAM_BIND}" \
  --tpu-ip 127.0.0.1 \
  --tpu-port "${BAM_TPU_PORT}" \
  --tpu-fwd-ip 127.0.0.1 \
  --tpu-fwd-port "${BAM_TPU_FWD_PORT}" \
  --shred-ip 127.0.0.1 \
  --shred-port "${BAM_SHRED_PORT}" \
  >"${BAM_LOG}" 2>&1 &
BAM_PID=$!

DEADLINE=$((SECONDS + 45))
wait_for_pattern "${FD_LOG}" 'BAM rx bundle: seq_id=1 .* txns=2 mode=atomic' "${DEADLINE}" \
  || die "validator log did not show the 2-transaction atomic BAM bundle"
wait_for_pattern "${BAM_LOG}" 'scheduler<-validator batch_result seq_id=1 status=not_committed reason=transaction_error' "${DEADLINE}" \
  || die "BAM log did not show a terminal not_committed transaction_error result"

POST_FROM=$(rpc_balance "${FROM}")
POST_TO1=$(rpc_balance "${TO1}")
POST_TO2=$(rpc_balance "${TO2}")

[[ "${POST_FROM}" == "${PRE_FROM}" ]] || die "payer balance changed despite atomic revert"
[[ "${POST_TO1}" == "${PRE_TO1}" ]] || die "recipient 1 balance changed despite atomic revert"
[[ "${POST_TO2}" == "${PRE_TO2}" ]] || die "recipient 2 balance changed despite atomic revert"

echo "post from: ${POST_FROM}"
echo "post to1:  ${POST_TO1}"
echo "post to2:  ${POST_TO2}"
echo "validated: atomic BAM batch reverted end to end on live fddev"
echo "artifacts: ${LOG_DIR}"
