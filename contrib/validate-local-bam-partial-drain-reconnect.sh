#!/usr/bin/env bash

# Validates a richer reconnect path:
# 1. start local fddev with RPC enabled;
# 2. generate several valid locally adapted BAM transfers;
# 3. send them as separate scheduler batches;
# 4. close the scheduler stream after the first terminal batch result;
# 5. confirm the validator reconnects and drains later batch results on conn=2.

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
BATCH_COUNT=8
LAMPORTS=12345
CU_LIMIT=300000
MAX_SCHEDULE_SLOT=max
CLOSE_AFTER_RESULTS=1
FROM_GENESIS_ACCOUNT=0
FIRST_TO_GENESIS_ACCOUNT=1
USE_SUDO="${USE_SUDO:-1}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --config PATH                  fddev config path (default: contrib/local-bam-compact.toml)
  --rpc-url URL                  JSON RPC URL (default: http://127.0.0.1:8899)
  --bam-bind HOST:PORT           BAM test-server bind address
  --bam-tpu-port PORT            TPU port advertised by BAM test server
  --bam-tpu-fwd-port PORT        TPU forward port advertised by BAM
  --bam-shred-port PORT          Shred port advertised by BAM
  --log-dir PATH                 Artifact directory (default: mktemp)
  --batch-count N                Number of valid BAM batches to queue (default: 8)
  --lamports N                   Lamports per transfer (default: 12345)
  --cu-limit N                   Compute-unit limit per transfer (default: 300000)
  --max-schedule-slot VALUE      max_schedule_slot literal for generated batches
                                  (default: max; supports max, leader, or leader+N)
  --close-after-results N        Wait for N batch results before sending the remaining
                                  burst and closing the stream (default: 1; 0 means no wait)
  --from-genesis-account N       Genesis-funded payer index (default: 0)
  --first-to-genesis-account N   First recipient genesis index (default: 1)
  -h, --help                     Show this help
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
    --batch-count)
      BATCH_COUNT="$2"
      shift 2
      ;;
    --lamports)
      LAMPORTS="$2"
      shift 2
      ;;
    --cu-limit)
      CU_LIMIT="$2"
      shift 2
      ;;
    --max-schedule-slot)
      MAX_SCHEDULE_SLOT="$2"
      shift 2
      ;;
    --close-after-results)
      CLOSE_AFTER_RESULTS="$2"
      shift 2
      ;;
    --from-genesis-account)
      FROM_GENESIS_ACCOUNT="$2"
      shift 2
      ;;
    --first-to-genesis-account)
      FIRST_TO_GENESIS_ACCOUNT="$2"
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
[[ "${BATCH_COUNT}" =~ ^[0-9]+$ && "${BATCH_COUNT}" -gt 1 ]] || die "--batch-count must be an integer greater than 1"
[[ "${CLOSE_AFTER_RESULTS}" =~ ^[0-9]+$ ]] || die "--close-after-results must be a non-negative integer"
(( CLOSE_AFTER_RESULTS <= BATCH_COUNT )) || die "--close-after-results must be <= --batch-count"
command -v cargo >/dev/null 2>&1 || die "cargo not found"
command -v curl >/dev/null 2>&1 || die "curl not found"
command -v jq >/dev/null 2>&1 || die "jq not found"

SUDO=()
if [[ "${USE_SUDO}" != "0" && "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo not found"
  SUDO=( sudo )
fi

if [[ -z "${LOG_DIR}" ]]; then
  LOG_DIR=$(mktemp -d /tmp/firebam-live-partial-drain.XXXXXX)
fi
mkdir -p "${LOG_DIR}"

FD_LOG="${LOG_DIR}/fd.log"
BAM_LOG="${LOG_DIR}/bam.log"
RPC_LOG="${LOG_DIR}/rpc.log"
FD_PID_FILE="${LOG_DIR}/fd.pid"
INPUT_PATH="${LOG_DIR}/input.txnctx"
SCENARIO_PATH="${LOG_DIR}/scenario.toml"

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

latest_batch_result_status() {
  local seq_id="$1"
  sed -n "s/.*scheduler<-validator batch_result seq_id=${seq_id} status=\\([^ ]*\\).*/\\1/p" "${BAM_LOG}" \
    | tail -n 1
}

extract_bridge_field() {
  local field="$1"
  local line="$2"
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

write_max_schedule_slot() {
  local value="$1"
  if [[ "${value}" =~ ^[0-9]+$ ]]; then
    printf 'max_schedule_slot = %s\n' "${value}"
  else
    printf 'max_schedule_slot = "%s"\n' "${value}"
  fi
}

echo "logs: ${LOG_DIR}"
echo "config: ${CONFIG}"
echo "rpc: ${RPC_URL}"
echo "batch count: ${BATCH_COUNT}"
echo "lamports: ${LAMPORTS}"
echo "max_schedule_slot: ${MAX_SCHEDULE_SLOT}"
echo "close_after_results: ${CLOSE_AFTER_RESULTS}"

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

cargo run --quiet --manifest-path "${BRIDGE_MANIFEST}" --bin gen_simple_system_txnctx -- \
  --output "${INPUT_PATH}" \
  --lamports "${LAMPORTS}" \
  --cu-limit "${CU_LIMIT}" \
  >"${LOG_DIR}/gen.out"

FROM=""
LAMPORTS_ACTUAL=""
PRE_FROM=""
declare -a RECIPIENTS=()
declare -a PRE_TO=()
declare -a EXPECTED_TO=()

{
  cat <<EOF
	description = "Send several valid BAM transfers, wait for a configured number of batch results, then close the scheduler stream and observe later batch results on the reconnect."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

EOF
} > "${SCENARIO_PATH}"

for batch_idx in $(seq 0 $((BATCH_COUNT - 1))); do
  seq_id=$((200 + batch_idx))
  to_genesis_account=$((FIRST_TO_GENESIS_ACCOUNT + batch_idx))
  packet_file="${LOG_DIR}/packet_${seq_id}.txt"
  bridge_out="${LOG_DIR}/bridge_${seq_id}.out"

  cargo run --quiet --manifest-path "${BRIDGE_MANIFEST}" --bin txnctx-bridge -- \
    --input "${INPUT_PATH}" \
    --output "${packet_file}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${BLOCKHASH}" \
    --from-genesis-account "${FROM_GENESIS_ACCOUNT}" \
    --to-genesis-account "${to_genesis_account}" \
    >"${bridge_out}"

  adapt_line=$(grep '^adapted family=' "${bridge_out}" || true)
  [[ -n "${adapt_line}" ]] || die "bridge did not emit adapted summary for seq_id=${seq_id}"

  from_pubkey=$(extract_bridge_field from "${adapt_line}")
  to_pubkey=$(extract_bridge_field to "${adapt_line}")
  lamports_value=$(extract_bridge_field lamports "${adapt_line}")
  [[ -n "${from_pubkey}" && -n "${to_pubkey}" && -n "${lamports_value}" ]] || die "failed to parse adapted transfer summary for seq_id=${seq_id}"

  if [[ -z "${FROM}" ]]; then
    FROM="${from_pubkey}"
    LAMPORTS_ACTUAL="${lamports_value}"
    PRE_FROM=$(rpc_balance "${FROM}")
  else
    [[ "${FROM}" == "${from_pubkey}" ]] || die "payer changed unexpectedly between adapted transfers"
    [[ "${LAMPORTS_ACTUAL}" == "${lamports_value}" ]] || die "lamports changed unexpectedly between adapted transfers"
  fi

  recipient_pre=$(rpc_balance "${to_pubkey}")
  RECIPIENTS+=( "${to_pubkey}" )
  PRE_TO+=( "${recipient_pre}" )
  EXPECTED_TO+=( "$((recipient_pre + lamports_value))" )

  {
    cat <<EOF
[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_file}"
revert_on_error = true
simple_vote_tx = false

EOF
    write_max_schedule_slot "${MAX_SCHEDULE_SLOT}"
    cat <<EOF

EOF
  } >> "${SCENARIO_PATH}"

  if (( CLOSE_AFTER_RESULTS > 0 && batch_idx + 1 == CLOSE_AFTER_RESULTS )); then
    cat >> "${SCENARIO_PATH}" <<EOF
[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = ${CLOSE_AFTER_RESULTS}
timeout_ms = 30000

EOF
  fi
done

{
  cat <<EOF
[[events]]
type = "close_stream"
EOF
} >> "${SCENARIO_PATH}"

echo "blockhash: ${BLOCKHASH}"
echo "from: ${FROM} pre=${PRE_FROM}"
echo "lamports per transfer: ${LAMPORTS_ACTUAL}"

for idx in "${!RECIPIENTS[@]}"; do
  echo "recipient[$idx]: ${RECIPIENTS[idx]} pre=${PRE_TO[idx]} expected=${EXPECTED_TO[idx]}"
done

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

if (( CLOSE_AFTER_RESULTS > 0 )); then
  wait_for_pattern "${BAM_LOG}" 'scheduler<-validator batch_result seq_id=.* conn=1' 60 \
    || die "did not observe an initial conn=1 batch result"
fi
wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' 60 \
  || die "validator did not reconnect for conn=2"
wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 replay disabled; not replaying' 60 \
  || die "conn=2 did not honor replay_on_reconnect=false"
if (( CLOSE_AFTER_RESULTS < BATCH_COUNT )); then
  wait_for_pattern "${BAM_LOG}" 'scheduler<-validator batch_result seq_id=.* conn=2' 60 \
    || die "did not observe a conn=2 batch result after reconnect"
fi

deadline=$((SECONDS + 60))
batch_result_count=0
while (( SECONDS < deadline )); do
  batch_result_count=$(grep -c 'scheduler<-validator batch_result seq_id=' "${BAM_LOG}" || true)
  if (( batch_result_count >= BATCH_COUNT )); then
    break
  fi
  sleep 1
done
[[ "${batch_result_count}" -ge "${BATCH_COUNT}" ]] || die "expected at least ${BATCH_COUNT} terminal batch results, saw ${batch_result_count}"

declare -a EXPECTED_FINAL_TO=()
committed_count=0
not_committed_count=0
for idx in "${!RECIPIENTS[@]}"; do
  seq_id=$((200 + idx))
  status=$(latest_batch_result_status "${seq_id}")
  case "${status}" in
    committed)
      EXPECTED_FINAL_TO+=( "${EXPECTED_TO[idx]}" )
      committed_count=$((committed_count + 1))
      ;;
    not_committed)
      EXPECTED_FINAL_TO+=( "${PRE_TO[idx]}" )
      not_committed_count=$((not_committed_count + 1))
      ;;
    "")
      die "missing terminal batch result for seq_id=${seq_id}"
      ;;
    *)
      die "unexpected terminal status for seq_id=${seq_id}: ${status}"
      ;;
  esac
done

deadline=$((SECONDS + 60))
all_balances_ok=0
while (( SECONDS < deadline )); do
  all_balances_ok=1
  for idx in "${!RECIPIENTS[@]}"; do
    post_to=$(rpc_balance "${RECIPIENTS[idx]}" || echo "${PRE_TO[idx]}")
    if [[ "${post_to}" != "${EXPECTED_FINAL_TO[idx]}" ]]; then
      all_balances_ok=0
      break
    fi
  done
  if (( all_balances_ok )); then
    break
  fi
  sleep 1
done

[[ "${all_balances_ok}" == "1" ]] || die "not all recipient balances reached terminal-status-derived expected values"

echo "validated: partial-drain reconnect produced terminal batch results on conn=2 committed=${committed_count} not_committed=${not_committed_count}"
echo "artifacts: ${LOG_DIR}"
