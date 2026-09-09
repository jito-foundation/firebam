#!/usr/bin/env bash

# Validates the current live repeated-seq_id split behavior:
# 1. start local fddev with RPC enabled;
# 2. generate six unique locally adapted BAM transfers;
# 3. send them as two atomic batches under the same seq_id with a [5,1] split;
# 4. confirm fddev decodes both fragments;
# 5. confirm the current branch commits them as two separate results and stays live.

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
BASE_LAMPORTS=12345
CU_LIMIT=300000
FROM_GENESIS_ACCOUNT=0
TO_GENESIS_ACCOUNT=1
SEQ_ID=500
USE_SUDO="${USE_SUDO:-1}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --config PATH                fddev config path (default: contrib/local-bam-compact.toml)
  --rpc-url URL                JSON RPC URL (default: http://127.0.0.1:8899)
  --bam-bind HOST:PORT         BAM test-server bind address
  --bam-tpu-port PORT          TPU port advertised by BAM test server
  --bam-tpu-fwd-port PORT      TPU forward port advertised by BAM
  --bam-shred-port PORT        Shred port advertised by BAM
  --log-dir PATH               Artifact directory (default: mktemp)
  --base-lamports N            Base lamports for the generated transfers (default: 12345)
  --cu-limit N                 Compute-unit limit per transfer (default: 300000)
  --from-genesis-account N     Genesis-funded payer index (default: 0)
  --to-genesis-account N       Recipient genesis index (default: 1)
  --seq-id N                   Reused BAM seq_id for the [5,1] split regression check (default: 500)
  -h, --help                   Show this help
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
    --base-lamports)
      BASE_LAMPORTS="$2"
      shift 2
      ;;
    --cu-limit)
      CU_LIMIT="$2"
      shift 2
      ;;
    --from-genesis-account)
      FROM_GENESIS_ACCOUNT="$2"
      shift 2
      ;;
    --to-genesis-account)
      TO_GENESIS_ACCOUNT="$2"
      shift 2
      ;;
    --seq-id)
      SEQ_ID="$2"
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
  LOG_DIR=$(mktemp -d /tmp/firebam-live-duplicate-seq.XXXXXX)
fi
mkdir -p "${LOG_DIR}"

FD_LOG="${LOG_DIR}/fd.log"
BAM_LOG="${LOG_DIR}/bam.log"
RPC_LOG="${LOG_DIR}/rpc.log"
FD_PID_FILE="${LOG_DIR}/fd.pid"
PACKETS_PATH="${LOG_DIR}/packets_500.txt"
SCENARIO_PATH="${LOG_DIR}/scenario.toml"
SUMMARY_PATH="${LOG_DIR}/packets.summary"

cleanup() {
  trap - EXIT INT TERM
  set +e
  [[ -n "${FD_PID:-}" ]] && "${SUDO[@]}" kill -- "-${FD_PID}" 2>/dev/null || true
  [[ -n "${FD_SUPERVISOR_PID:-}" ]] && kill "${FD_SUPERVISOR_PID}" 2>/dev/null || true
  [[ -n "${BAM_PID:-}" ]] && kill "${BAM_PID}" 2>/dev/null || true
  [[ -n "${FD_SUPERVISOR_PID:-}" ]] && wait "${FD_SUPERVISOR_PID}" 2>/dev/null || true
  [[ -n "${BAM_PID:-}" ]] && wait "${BAM_PID}" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

rpc_latest_blockhash() {
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash","params":[{"commitment":"processed"}]}' \
    | tee -a "${RPC_LOG}" \
    | jq -e -r '.result.value.blockhash'
}

wait_for_pattern() {
  local file="$1"
  local pattern="$2"
  local timeout_secs="$3"
  local deadline=$((SECONDS + timeout_secs))
  while :; do
    if grep -qE "${pattern}" "${file}" 2>/dev/null; then
      return 0
    fi
    if [[ -n "${BAM_PID:-}" ]] && ! kill -0 "${BAM_PID}" 2>/dev/null; then
      break
    fi
    if [[ -n "${FD_SUPERVISOR_PID:-}" ]] && ! kill -0 "${FD_SUPERVISOR_PID}" 2>/dev/null; then
      break
    fi
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 1
  done
  return 1
}

echo "logs: ${LOG_DIR}"
echo "config: ${CONFIG}"
echo "rpc: ${RPC_URL}"
echo "seq_id: ${SEQ_ID}"
echo "base lamports: ${BASE_LAMPORTS}"

"${SUDO[@]}" "${FDDEV}" configure init all --config "${CONFIG}" >/dev/null 2>&1

rm -f "${FD_PID_FILE}"
"${SUDO[@]}" bash -lc '
  pid_file="$1"
  fddev_bin="$2"
  config_path="$3"
  log_file="$4"
  setsid "${fddev_bin}" dev --no-watch --no-configure --config "${config_path}" > "${log_file}" 2>&1 &
  child_pid=$!
  echo "${child_pid}" > "${pid_file}"
  wait "${child_pid}"
' bash "${FD_PID_FILE}" "${FDDEV}" "${CONFIG}" "${FD_LOG}" &
FD_SUPERVISOR_PID=$!

for _ in $(seq 1 50); do
  if [[ -s "${FD_PID_FILE}" ]]; then
    FD_PID=$(tr -d '[:space:]' < "${FD_PID_FILE}")
    break
  fi
  sleep 0.2
done
[[ -n "${FD_PID:-}" ]] || die "failed to capture fddev pid"

BLOCKHASH=""
for _ in $(seq 1 60); do
  if BLOCKHASH=$(rpc_latest_blockhash 2>/dev/null); then
    break
  fi
  sleep 1
done
[[ -n "${BLOCKHASH}" ]] || die "validator RPC did not become ready at ${RPC_URL}"

: >"${PACKETS_PATH}"
: >"${SUMMARY_PATH}"
for idx in $(seq 0 5); do
  lamports=$((BASE_LAMPORTS + idx))
  input_path="${LOG_DIR}/input_${idx}.txnctx"
  bridge_out="${LOG_DIR}/bridge_${idx}.out"
  packet_path="${LOG_DIR}/packet_${idx}.txt"

  cargo run --quiet --manifest-path "${BRIDGE_MANIFEST}" --bin gen_simple_system_txnctx -- \
    --output "${input_path}" \
    --lamports "${lamports}" \
    --cu-limit "${CU_LIMIT}" \
    >"${LOG_DIR}/gen_${idx}.out"

  cargo run --quiet --manifest-path "${BRIDGE_MANIFEST}" --bin txnctx-bridge -- \
    --input "${input_path}" \
    --output "${packet_path}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${BLOCKHASH}" \
    --from-genesis-account "${FROM_GENESIS_ACCOUNT}" \
    --to-genesis-account "${TO_GENESIS_ACCOUNT}" \
    >"${bridge_out}"

  cat "${packet_path}" >>"${PACKETS_PATH}"
  grep '^adapted family=' "${bridge_out}" >>"${SUMMARY_PATH}" || true
done

PACKET_COUNT=$(wc -l < "${PACKETS_PATH}")
[[ "${PACKET_COUNT}" == "6" ]] || die "expected 6 packet lines, found ${PACKET_COUNT}"

echo "blockhash: ${BLOCKHASH}"
echo "packet_count: ${PACKET_COUNT}"
echo "adapted summaries:"
cat "${SUMMARY_PATH}"

cat > "${SCENARIO_PATH}" <<EOF
description = "Send six unique transfers as a [5,1] same-seq_id split and confirm the current branch treats them as separate atomic batches."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_split_batch"
seq_id = ${SEQ_ID}
splits = [5, 1]
packets_base64_file = "${PACKETS_PATH}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "sleep"
ms = 4000
EOF

cargo run --quiet --manifest-path "${BAM_MANIFEST}" --bin bam-test-server -- \
  --scenario benign \
  --scenario-file "${SCENARIO_PATH}" \
  --bind "${BAM_BIND}" \
  --tpu-ip 127.0.0.1 \
  --tpu-port "${BAM_TPU_PORT}" \
  --tpu-fwd-ip 127.0.0.1 \
  --tpu-fwd-port "${BAM_TPU_FWD_PORT}" \
  --shred-ip 127.0.0.1 \
  --shred-port "${BAM_SHRED_PORT}" \
  >"${BAM_LOG}" 2>&1 &
BAM_PID=$!

wait_for_pattern "${BAM_LOG}" "scripted send_split_batch seq_id=${SEQ_ID} splits=\[5, 1\] max_schedule_slot=18446744073709551615" 60 \
  || die "controller did not emit the scripted same-seq split log"
wait_for_pattern "${FD_LOG}" "BAM rx bundle: seq_id=${SEQ_ID} max_schedule_slot=18446744073709551615 txns=5 mode=atomic" 60 \
  || die "validator log did not show the first same-seq split fragment"
wait_for_pattern "${FD_LOG}" "BAM rx bundle: seq_id=${SEQ_ID} max_schedule_slot=18446744073709551615 txns=1 mode=atomic" 60 \
  || die "validator log did not show the second same-seq split fragment"
wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ID} status=committed txns=5 conn=1" 60 \
  || die "missing committed result for the first same-seq split fragment"
wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ID} status=committed txns=1 conn=1" 60 \
  || die "missing committed result for the second same-seq split fragment"

if grep -q 'bundle_idx 5 > 4' "${FD_LOG}"; then
  die "historical dedup overflow still reproduced unexpectedly"
fi
if grep -q 'tile dedup:0 exited with code 1' "${FD_LOG}"; then
  die "dedup tile exited unexpectedly during same-seq split regression check"
fi
kill -0 "${FD_SUPERVISOR_PID}" 2>/dev/null || die "fddev supervisor exited unexpectedly during same-seq split regression check"
kill -0 "${FD_PID}" 2>/dev/null || die "fddev child exited unexpectedly during same-seq split regression check"

FIRST_RESULT=$(grep "scheduler<-validator batch_result seq_id=${SEQ_ID} status=committed txns=5 conn=1" "${BAM_LOG}" | head -n 1 || true)
SECOND_RESULT=$(grep "scheduler<-validator batch_result seq_id=${SEQ_ID} status=committed txns=1 conn=1" "${BAM_LOG}" | head -n 1 || true)
[[ -n "${FIRST_RESULT}" && -n "${SECOND_RESULT}" ]] || die "failed to capture committed result evidence lines"

echo "result[5]: ${FIRST_RESULT}"
echo "result[1]: ${SECOND_RESULT}"
echo "validated: current branch accepts repeated seq_id [5,1] split as two committed BAM batches without dedup crash"
echo "artifacts: ${LOG_DIR}"
