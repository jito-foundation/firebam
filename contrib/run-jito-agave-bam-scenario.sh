#!/usr/bin/env bash

# Run one existing BAM scripted scenario against the jito-solana BAM client.
#
# This is the first comparison-target adapter.  It intentionally consumes a
# scenario generated elsewhere, so the same packet files can be replayed against
# another BAM client and normalized with contrib/normalize-bam-outcome.py.

set -euo pipefail
IFS=$'\n\t'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)

JITO_SOLANA_DIR="${JITO_SOLANA_DIR:-${HOME}/jito-solana}"
JITO_PROFILE="${JITO_PROFILE:-release}"
LOG_DIR=""
SCENARIO_FILE=""
SOURCE_SUMMARY=""
TARGET_NAME="${TARGET_NAME:-jito-agave}"
RPC_PORT="${RPC_PORT:-8899}"
RPC_URL="${RPC_URL:-}"
GOSSIP_PORT="${GOSSIP_PORT:-}"
TIMEOUT_SECS=150
CAPTURE_BLOCK_SLOT_LIMIT="${CAPTURE_BLOCK_SLOT_LIMIT:-256}"
CAPTURE_RPC_EVIDENCE="${CAPTURE_RPC_EVIDENCE:-1}"
BURST_TAIL_RESULT_TIMEOUT_SECS="${BURST_TAIL_RESULT_TIMEOUT_SECS:-30}"
BALANCE_OBSERVE_TIMEOUT_SECS="${BALANCE_OBSERVE_TIMEOUT_SECS:-}"
BUILD=0
BAM_BIND="127.0.0.1:50055"
BAM_URL="http://127.0.0.1:50055"
BAM_BAD_URL="http://127.0.0.1:50056"
TPU_PORT=9007
TPU_FWD_PORT=9008
SHRED_PORT=9009
FAUCET_PORT=9900
SLOTS_PER_EPOCH=10000
LIMIT_LEDGER_SIZE=10000
PAYER_PREFUND_LAMPORTS="${PAYER_PREFUND_LAMPORTS:-500000000000000}"
BAM_BUILDER_COMMISSION_PCT="${BAM_BUILDER_COMMISSION_PCT:-3}"
BAM_COMMISSION_BPS="${BAM_COMMISSION_BPS:-300}"
BAM_DISABLED_WORKLOAD_SCRIPT="${BAM_DISABLED_WORKLOAD_SCRIPT:-}"
TICKS_PER_SLOT=""

usage() {
  cat <<EOF2
Usage: $(basename "$0") --scenario-file PATH --source-summary PATH [OPTIONS]

Options:
  --jito-solana-dir PATH       jito-solana checkout (default: ~/jito-solana)
  --profile NAME               Cargo profile containing built binaries (default: release)
  --build                      Build jito-solana binaries before running
  --log-dir PATH               Artifact directory (default: mktemp)
  --scenario-file PATH         Scripted BAM scenario to replay
  --source-summary PATH        summary.txt from the source fddev iteration
  --rpc-port PORT              Agave RPC port (default: 8899)
  --rpc-url URL                Agave RPC URL (default: http://127.0.0.1:8899)
  --gossip-port PORT           Agave gossip port (default: rpc-port + 140)
  --bam-bind HOST:PORT         BAM test-server bind address
  --bam-url URL                BAM URL configured into Agave
  --bam-bad-url URL            Temporary bad BAM URL for URL churn
  --tpu-port PORT              TPU port advertised by BAM test server
  --tpu-fwd-port PORT          TPU forward port advertised by BAM
  --shred-port PORT            Shred port advertised by BAM
  --faucet-port PORT           Faucet port (default: 9900)
  --ticks-per-slot N          Override Agave ticks per slot
  --target-name NAME           Outcome target label (default: jito-agave)
  --timeout-secs N             Wait timeout (default: 150)
  --burst-tail-result-timeout-secs N
                              Timeout for optional last-result waits in
                              large burst modes (default: 30)
  -h, --help                   Show this help
EOF2
}

die() {
  echo "error: $*" >&2
  exit 1
}

while (($#)); do
  case "$1" in
    --jito-solana-dir)
      JITO_SOLANA_DIR="$2"
      shift 2
      ;;
    --profile)
      JITO_PROFILE="$2"
      shift 2
      ;;
    --build)
      BUILD=1
      shift
      ;;
    --log-dir)
      LOG_DIR="$2"
      shift 2
      ;;
    --scenario-file)
      SCENARIO_FILE="$2"
      shift 2
      ;;
    --source-summary)
      SOURCE_SUMMARY="$2"
      shift 2
      ;;
    --rpc-port)
      RPC_PORT="$2"
      shift 2
      ;;
    --rpc-url)
      RPC_URL="$2"
      shift 2
      ;;
    --gossip-port)
      GOSSIP_PORT="$2"
      shift 2
      ;;
    --bam-bind)
      BAM_BIND="$2"
      shift 2
      ;;
    --bam-url)
      BAM_URL="$2"
      shift 2
      ;;
    --bam-bad-url)
      BAM_BAD_URL="$2"
      shift 2
      ;;
    --tpu-port)
      TPU_PORT="$2"
      shift 2
      ;;
    --tpu-fwd-port)
      TPU_FWD_PORT="$2"
      shift 2
      ;;
    --shred-port)
      SHRED_PORT="$2"
      shift 2
      ;;
    --faucet-port)
      FAUCET_PORT="$2"
      shift 2
      ;;
    --ticks-per-slot)
      TICKS_PER_SLOT="$2"
      shift 2
      ;;
    --target-name)
      TARGET_NAME="$2"
      shift 2
      ;;
    --timeout-secs)
      TIMEOUT_SECS="$2"
      shift 2
      ;;
    --burst-tail-result-timeout-secs)
      BURST_TAIL_RESULT_TIMEOUT_SECS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

if [[ -z "${RPC_URL}" ]]; then
  RPC_URL="http://127.0.0.1:${RPC_PORT}"
fi
if [[ -z "${GOSSIP_PORT}" ]]; then
  GOSSIP_PORT=$((RPC_PORT + 140))
  if (( GOSSIP_PORT > 65535 )); then
    GOSSIP_PORT=0
  fi
fi

choose_free_tcp_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

if [[ "${FAUCET_PORT}" == "0" ]]; then
  FAUCET_PORT=$(choose_free_tcp_port)
fi
if [[ -z "${BALANCE_OBSERVE_TIMEOUT_SECS}" ]]; then
  BALANCE_OBSERVE_TIMEOUT_SECS="${TIMEOUT_SECS}"
fi

regex_escape() {
  printf '%s' "$1" | sed -e 's/[][(){}.^$*+?|\\/]/\\&/g'
}

BAM_URL_RE=$(regex_escape "${BAM_URL}")
BAM_BAD_URL_RE=$(regex_escape "${BAM_BAD_URL}")
RPC_URL_RE=$(regex_escape "${RPC_URL}")

[[ -n "${SCENARIO_FILE}" ]] || die "--scenario-file is required"
[[ -n "${SOURCE_SUMMARY}" ]] || die "--source-summary is required"
[[ -f "${SCENARIO_FILE}" ]] || die "missing scenario file ${SCENARIO_FILE}"
[[ -f "${SOURCE_SUMMARY}" ]] || die "missing source summary ${SOURCE_SUMMARY}"
[[ -d "${JITO_SOLANA_DIR}" ]] || die "missing jito-solana checkout ${JITO_SOLANA_DIR}"

BAM_MANIFEST="${ROOT}/contrib/bam-test-server/Cargo.toml"
BRIDGE_MANIFEST="${ROOT}/contrib/txnctx-bridge/Cargo.toml"
BRIDGE_BIN="${ROOT}/contrib/txnctx-bridge/target/debug/txnctx-bridge"
GEN_SIMPLE_SYSTEM_TXNCTX_BIN="${ROOT}/contrib/txnctx-bridge/target/debug/gen_simple_system_txnctx"
GEN_PROGRAM_INVOCATION_BIN="${ROOT}/contrib/txnctx-bridge/target/debug/gen_program_invocation"
GEN_DURABLE_NONCE_TRANSFER_BIN="${ROOT}/contrib/txnctx-bridge/target/debug/gen_durable_nonce_transfer"
WRITE_SEEDED_KEYPAIR_BIN="${ROOT}/contrib/txnctx-bridge/target/debug/write_seeded_keypair"
MAKE_ALT_SETUP_TXNS_BIN="${ROOT}/contrib/txnctx-bridge/target/debug/make_alt_setup_txns"
NORMALIZER="${ROOT}/contrib/normalize-bam-outcome.py"
CAPTURE_CHAIN_EVIDENCE="${ROOT}/contrib/capture-bam-chain-evidence.py"
MATERIALIZE_SCENARIO="${ROOT}/contrib/materialize-bam-scenario-packets.py"
FDDEV="${FDDEV:-${ROOT}/build/native/gcc/bin/fddev}"
if [[ ! -x "${FDDEV}" && -x "${ROOT}/build/native/gcc/bin/firedancer-dev" ]]; then
  FDDEV="${ROOT}/build/native/gcc/bin/firedancer-dev"
fi
if [[ "${FDDEV##*/}" == "firedancer-dev" ]]; then
  FDDEV_DEFAULT_TXN_CONFIG="${ROOT}/contrib/local-bam-fullfd-compact.toml"
else
  FDDEV_DEFAULT_TXN_CONFIG="${ROOT}/contrib/local-bam-compact.toml"
fi
FDDEV_TXN_CONFIG="${FDDEV_TXN_CONFIG:-${FDDEV_DEFAULT_TXN_CONFIG}}"
[[ -f "${BAM_MANIFEST}" ]] || die "missing ${BAM_MANIFEST}"
[[ -f "${BRIDGE_MANIFEST}" ]] || die "missing ${BRIDGE_MANIFEST}"
[[ -x "${NORMALIZER}" ]] || die "missing executable ${NORMALIZER}"
[[ -x "${CAPTURE_CHAIN_EVIDENCE}" ]] || die "missing executable ${CAPTURE_CHAIN_EVIDENCE}"
[[ -x "${MATERIALIZE_SCENARIO}" ]] || die "missing executable ${MATERIALIZE_SCENARIO}"
command -v cargo >/dev/null 2>&1 || die "cargo not found"
command -v curl >/dev/null 2>&1 || die "curl not found"
command -v jq >/dev/null 2>&1 || die "jq not found"
if [[ -n "${BAM_DISABLED_WORKLOAD_SCRIPT}" ]]; then
  [[ -x "${BAM_DISABLED_WORKLOAD_SCRIPT}" ]] \
    || die "missing executable BAM-disabled workload ${BAM_DISABLED_WORKLOAD_SCRIPT}"
fi

if [[ -z "${LOG_DIR}" ]]; then
  LOG_DIR=$(mktemp -d /tmp/firebam-jito-agave-scenario.XXXXXX)
fi
mkdir -p "${LOG_DIR}"

profile_flag=()
if [[ "${JITO_PROFILE}" == "release" ]]; then
  profile_flag=( --release )
fi
VALIDATOR_BUILD_PATH="${JITO_SOLANA_DIR}/target/${JITO_PROFILE}"

if [[ "${BUILD}" == "1" ]]; then
  (
    cd "${JITO_SOLANA_DIR}"
    cargo build "${profile_flag[@]}" \
      --bin solana \
      --bin solana-keygen \
      --bin solana-test-validator \
      --bin agave-validator
  )
fi

if [[ -d "${VALIDATOR_BUILD_PATH}" ]]; then
  export PATH="${VALIDATOR_BUILD_PATH}:${PATH}"
fi
command -v solana >/dev/null 2>&1 || die "solana not found"
command -v solana-keygen >/dev/null 2>&1 || die "solana-keygen not found"

SOLANA_TEST_VALIDATOR_BIN="${VALIDATOR_BUILD_PATH}/solana-test-validator"
AGAVE_VALIDATOR_BIN="${VALIDATOR_BUILD_PATH}/agave-validator"

[[ -x "${SOLANA_TEST_VALIDATOR_BIN}" ]] || die "missing ${SOLANA_TEST_VALIDATOR_BIN}; rerun with --build"

summary_get() {
  local key="$1"
  awk -F= -v key="${key}" '$1 == key { print substr($0, length(key) + 2); exit }' "${SOURCE_SUMMARY}"
}

rpc_latest_blockhash() {
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash","params":[{"commitment":"processed"}]}' \
    | jq -e -r '.result.value.blockhash'
}

rpc_health() {
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}'
}

rpc_slot() {
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"processed"}]}' \
    | jq -e -r '.result'
}

rpc_finalized_slot() {
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"finalized"}]}' \
    | jq -e -r '.result'
}

rpc_signature_status() {
  local sig="$1"
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getSignatureStatuses\",\"params\":[[\"${sig}\"],{\"searchTransactionHistory\":true}]}"
}

rpc_block_signatures() {
  local slot="$1"
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBlock\",\"params\":[${slot},{\"encoding\":\"json\",\"transactionDetails\":\"signatures\",\"rewards\":false,\"maxSupportedTransactionVersion\":0}]}"
}

capture_rpc_blocks() {
  local start_slot="$1"
  local end_slot="$2"
  local output="$3"
  : >"${output}"
  [[ "${start_slot}" =~ ^[0-9]+$ && "${end_slot}" =~ ^[0-9]+$ ]] || return 0
  (( end_slot >= start_slot )) || return 0

  local slot response compact deadline
  for slot in $(seq "${start_slot}" "${end_slot}"); do
    deadline=$((SECONDS + 2))
    response=""
    while :; do
      response=$(rpc_block_signatures "${slot}" 2>/dev/null || printf '{"error":{"message":"rpc_failed"}}')
      if printf '%s' "${response}" | jq -e '.result.signatures' >/dev/null 2>&1; then
        break
      fi
      if ! printf '%s' "${response}" | jq -e '.error.code == -32004' >/dev/null 2>&1; then
        break
      fi
      (( SECONDS < deadline )) || break
      sleep 0.5
    done
    compact=$(printf '%s' "${response}" | jq -c . 2>/dev/null || printf '{"error":{"message":"invalid_json"}}')
    printf '{"slot":%s,"response":%s}\n' "${slot}" "${compact}" >>"${output}"
  done
}

capture_rpc_signature_statuses() {
  local scenario_file="$1"
  local output="$2"
  local extra_packet_file="${3:-}"
  [[ -f "${scenario_file}" ]] || return 0
  local -a extra_args=()
  if [[ -n "${extra_packet_file}" && -f "${extra_packet_file}" ]]; then
    extra_args+=(--extra-packet-file "${extra_packet_file}")
  fi
  "${CAPTURE_CHAIN_EVIDENCE}" \
    --scenario-file "${scenario_file}" \
    --rpc-url "${RPC_URL}" \
    --output "${output}" \
    --timeout-secs 8 \
    "${extra_args[@]}"
}

rpc_balance() {
  local pubkey="$1"
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBalance\",\"params\":[\"${pubkey}\",{\"commitment\":\"processed\"}]}" \
    | jq -e -r '.result.value'
}

rpc_account_info() {
  local pubkey="$1"
  local commitment="${2:-processed}"
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAccountInfo\",\"params\":[\"${pubkey}\",{\"encoding\":\"base64\",\"commitment\":\"${commitment}\"}]}"
}

rpc_account_owner() {
  local pubkey="$1"
  rpc_account_info "${pubkey}" | jq -r '.result.value.owner // empty'
}

rpc_account_space() {
  local pubkey="$1"
  rpc_account_info "${pubkey}" | jq -r '.result.value.space // empty'
}

rpc_nonce_hash() {
  local nonce_pubkey="$1"
  solana --url "${RPC_URL}" nonce-account "${nonce_pubkey}" \
    --commitment processed \
    --output json \
    | jq -e -r '.nonce'
}

wait_for_nonce_hash() {
  local nonce_pubkey="$1"
  local timeout_secs="$2"
  local deadline=$((SECONDS + timeout_secs))
  local observed=""
  while (( SECONDS < deadline )); do
    observed=$(rpc_nonce_hash "${nonce_pubkey}" 2>/dev/null || true)
    if [[ -n "${observed}" && "${observed}" != "null" ]]; then
      printf '%s\n' "${observed}"
      return 0
    fi
    sleep 1
  done
  return 1
}

rpc_send_base64_transaction() {
  local encoded="$1"
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"sendTransaction\",\"params\":[\"${encoded}\",{\"encoding\":\"base64\",\"skipPreflight\":true,\"maxRetries\":20}]}" \
    | jq -e -r '.result'
}

rpc_simulate_base64_transaction_file() {
  local packet_file="$1"
  local encoded
  encoded=$(tr -d '[:space:]' < "${packet_file}")
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"simulateTransaction\",\"params\":[\"${encoded}\",{\"encoding\":\"base64\",\"sigVerify\":true,\"replaceRecentBlockhash\":false,\"commitment\":\"processed\"}]}"
}

wait_for_account_present() {
  local pubkey="$1"
  local timeout_secs="$2"
  local commitment="${3:-processed}"
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    if rpc_account_info "${pubkey}" "${commitment}" 2>/dev/null | jq -e '.result.value != null' >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

rpc_airdrop() {
  local pubkey="$1"
  local lamports="$2"
  curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"requestAirdrop\",\"params\":[\"${pubkey}\",${lamports}]}" \
    | jq -e -r '.result'
}

wait_rpc_ready() {
  local timeout_secs="$1"
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    if rpc_latest_blockhash >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_rpc_slot_at_least() {
  local minimum="$1"
  local timeout_secs="$2"
  local deadline=$((SECONDS + timeout_secs))
  local observed=0
  while (( SECONDS < deadline )); do
    observed=$(rpc_slot 2>/dev/null || printf '0')
    if [[ "${observed}" =~ ^[0-9]+$ ]] && (( observed >= minimum )); then
      printf '%s\n' "${observed}"
      return 0
    fi
    sleep 1
  done
  printf '%s\n' "${observed}"
  return 1
}

wait_rpc_finalized_slot_at_least() {
  local minimum="$1"
  local timeout_secs="$2"
  local deadline=$((SECONDS + timeout_secs))
  local observed=0
  while (( SECONDS < deadline )); do
    observed=$(rpc_finalized_slot 2>/dev/null || printf '0')
    if [[ "${observed}" =~ ^[0-9]+$ ]] && (( observed >= minimum )); then
      printf '%s\n' "${observed}"
      return 0
    fi
    sleep 1
  done
  printf '%s\n' "${observed}"
  return 1
}

wait_for_signature_ok() {
  local sig="$1"
  local timeout_secs="$2"
  local deadline=$((SECONDS + timeout_secs))
  local response=""
  while (( SECONDS < deadline )); do
    response=$(rpc_signature_status "${sig}" 2>/dev/null || printf '{}')
    if printf '%s' "${response}" | jq -e '.result.value[0] != null' >/dev/null 2>&1; then
      if printf '%s' "${response}" | jq -e '.result.value[0].err == null' >/dev/null 2>&1; then
        return 0
      fi
      printf 'signature %s failed: %s\n' "${sig}" "$(printf '%s' "${response}" | jq -c '.result.value[0].err')" >&2
      return 1
    fi
    sleep 1
  done
  printf 'signature %s did not reach a terminal status; last response: %s\n' "${sig}" "${response}" >&2
  return 1
}

wait_for_signature_finalized() {
  local sig="$1"
  local timeout_secs="$2"
  local deadline=$((SECONDS + timeout_secs))
  local response=""
  while (( SECONDS < deadline )); do
    response=$(rpc_signature_status "${sig}" 2>/dev/null || printf '{}')
    if printf '%s' "${response}" | jq -e '.result.value[0] != null' >/dev/null 2>&1; then
      if ! printf '%s' "${response}" | jq -e '.result.value[0].err == null' >/dev/null 2>&1; then
        printf 'signature %s failed before finalization: %s\n' "${sig}" "$(printf '%s' "${response}" | jq -c '.result.value[0].err')" >&2
        return 1
      fi
      if printf '%s' "${response}" | jq -e '.result.value[0].confirmationStatus == "finalized"' >/dev/null 2>&1; then
        printf '%s\n' "$(printf '%s' "${response}" | jq -e -r '.result.value[0].slot')"
        return 0
      fi
    fi
    sleep 1
  done
  printf 'signature %s did not finalize; last response: %s\n' "${sig}" "${response}" >&2
  return 1
}

wait_rpc_healthy() {
  local timeout_secs="$1"
  local deadline=$((SECONDS + timeout_secs))
  local response=""
  while (( SECONDS < deadline )); do
    response=$(rpc_health 2>/dev/null || printf '{}')
    if printf '%s' "${response}" | jq -e '.result == "ok"' >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  printf '%s\n' "${response}" >&2
  return 1
}

wait_for_balance_at_least() {
  local pubkey="$1"
  local minimum="$2"
  local timeout_secs="$3"
  local deadline=$((SECONDS + timeout_secs))
  local observed=0
  while (( SECONDS < deadline )); do
    observed=$(rpc_balance "${pubkey}" 2>/dev/null || printf '0')
    if (( observed >= minimum )); then
      printf '%s\n' "${observed}"
      return 0
    fi
    sleep 1
  done
  printf '%s\n' "${observed}"
  return 1
}

observe_balance_at_least_or_current() {
  local pubkey="$1"
  local minimum="$2"
  local timeout_secs="$3"
  local observed
  observed=$(wait_for_balance_at_least "${pubkey}" "${minimum}" "${timeout_secs}" 2>/dev/null \
    || rpc_balance "${pubkey}" 2>/dev/null \
    || true)
  printf '%s\n' "${observed}" | tail -n1
}

wait_for_pattern() {
  local file="$1"
  local pattern="$2"
  local timeout_secs="$3"
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    if grep -qE "${pattern}" "${file}" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

scenario_has_send_events() {
  local scenario_file="$1"
  grep -Eq 'type[[:space:]]*=[[:space:]]*"send_(batch|multi_batch|split_batch)"' "${scenario_file}" 2>/dev/null
}

metadata_get() {
  local metadata_file="$1"
  local key="$2"
  awk -F= -v key="${key}" '$1 == key { print substr($0, length(key) + 2); exit }' "${metadata_file}"
}

materialize_external_scenario() {
  local recipe="$1"
  local output="$2"
  local packet_dir="$3"
  local metadata="$4"
  local blockhash="$5"
  "${MATERIALIZE_SCENARIO}" \
    --scenario "${recipe}" \
    --output "${output}" \
    --packet-dir "${packet_dir}" \
    --metadata-output "${metadata}" \
    --gen-simple-system-txnctx "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --gen-program-invocation "${GEN_PROGRAM_INVOCATION_BIN}" \
    --bridge "${BRIDGE_BIN}" \
    --recent-blockhash "${blockhash}" \
    --from-seed-index 0 \
    --to-seed-start 1
}

wait_for_jito_tpu_quic_addr() {
  local fd_log="$1"
  local timeout_secs="$2"
  local deadline=$((SECONDS + timeout_secs))
  local addr=""
  while (( SECONDS < deadline )); do
    addr=$(sed -n 's/^TPU QUIC Address: //p' "${fd_log}" 2>/dev/null | tail -n 1)
    if [[ -n "${addr}" ]]; then
      printf '%s\n' "${addr}"
      return 0
    fi
    sleep 1
  done
  return 1
}

send_normal_tpu_packet_after_bam_commit() {
  local packet_path="$1"
  local bam_log="$2"
  local fd_log="$3"
  local seq_one="$4"
  local normal_tpu_log="$5"

  [[ -x "${FDDEV}" ]] || die "missing ${FDDEV}; build fddev before running source_mix_bam_tpu"
  [[ -f "${FDDEV_TXN_CONFIG}" ]] || die "missing fddev txn config ${FDDEV_TXN_CONFIG}"
  wait_for_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed" "${TIMEOUT_SECS}" \
    || die "BAM commit for seq_id=${seq_one} did not arrive before normal TPU injection"

  local addr
  addr=$(wait_for_jito_tpu_quic_addr "${fd_log}" "${TIMEOUT_SECS}") \
    || die "jito-agave did not print a TPU QUIC address"
  local dst_ip="${addr%:*}"
  local dst_port="${addr##*:}"
  [[ -n "${dst_ip}" && "${dst_port}" =~ ^[0-9]+$ ]] \
    || die "could not parse jito TPU QUIC address: ${addr}"

  local payload
  payload=$(tr -d '[:space:]' < "${packet_path}")
  [[ -n "${payload}" ]] || die "normal TPU packet payload is empty"

  NORMAL_TPU_LOG="${normal_tpu_log}"
  NORMAL_TPU_PACKET="${packet_path}"
  NORMAL_TPU_DST="${addr}"
  NORMAL_TPU_PORT="${dst_port}"
  {
    printf 'normal_tpu_dst=%s\n' "${NORMAL_TPU_DST}"
    printf 'normal_tpu_packet_file=%s\n' "${packet_path}"
    printf 'normal_tpu_after_bam_seq=%s\n' "${seq_one}"
    printf '\n'
  } >"${normal_tpu_log}"

  if ! "${FDDEV}" txn \
      --config "${FDDEV_TXN_CONFIG}" \
      --count 1 \
      --no-ready \
      --dst-ip "${dst_ip}" \
      --dst-port "${dst_port}" \
      --payload-base64-encoded "${payload}" \
      >>"${normal_tpu_log}" 2>&1; then
    echo "--- tail ${normal_tpu_log} ---" >&2
    tail -n 80 "${normal_tpu_log}" >&2 || true
    die "normal TPU injection failed"
  fi
}

send_normal_tpu_packet_after_scheduler_ready() {
  local packet_path="$1"
  local bam_log="$2"
  local fd_log="$3"
  local normal_tpu_log="$4"

  [[ -x "${FDDEV}" ]] || die "missing ${FDDEV}; build fddev before running source_mix_precommit"
  [[ -f "${FDDEV_TXN_CONFIG}" ]] || die "missing fddev txn config ${FDDEV_TXN_CONFIG}"
  wait_for_pattern "${bam_log}" '^InitSchedulerStream:' "${TIMEOUT_SECS}" \
    || die "BAM scheduler stream did not open before normal TPU precommit injection"

  local addr
  addr=$(wait_for_jito_tpu_quic_addr "${fd_log}" "${TIMEOUT_SECS}") \
    || die "jito-agave did not print a TPU QUIC address"
  local dst_ip="${addr%:*}"
  local dst_port="${addr##*:}"
  [[ -n "${dst_ip}" && "${dst_port}" =~ ^[0-9]+$ ]] \
    || die "could not parse jito TPU QUIC address: ${addr}"

  local payload
  payload=$(tr -d '[:space:]' < "${packet_path}")
  [[ -n "${payload}" ]] || die "normal TPU packet payload is empty"

  NORMAL_TPU_LOG="${normal_tpu_log}"
  NORMAL_TPU_PACKET="${packet_path}"
  NORMAL_TPU_DST="${addr}"
  NORMAL_TPU_PORT="${dst_port}"
  {
    printf 'normal_tpu_dst=%s\n' "${NORMAL_TPU_DST}"
    printf 'normal_tpu_packet_file=%s\n' "${packet_path}"
    printf 'normal_tpu_trigger=precommit\n'
    printf '\n'
  } >"${normal_tpu_log}"

  if ! "${FDDEV}" txn \
      --config "${FDDEV_TXN_CONFIG}" \
      --count 1 \
      --no-ready \
      --dst-ip "${dst_ip}" \
      --dst-port "${dst_port}" \
      --payload-base64-encoded "${payload}" \
      >>"${normal_tpu_log}" 2>&1; then
    echo "--- tail ${normal_tpu_log} ---" >&2
    tail -n 80 "${normal_tpu_log}" >&2 || true
    die "normal TPU precommit injection failed"
  fi
}

send_normal_tpu_packet_after_queue_close() {
  local packet_path="$1"
  local bam_log="$2"
  local fd_log="$3"
  local normal_tpu_log="$4"

  [[ -x "${FDDEV}" ]] || die "missing ${FDDEV}; build fddev before running queue source-mix scenarios"
  [[ -f "${FDDEV_TXN_CONFIG}" ]] || die "missing fddev txn config ${FDDEV_TXN_CONFIG}"
  wait_for_pattern "${bam_log}" 'scripted close_stream' "${TIMEOUT_SECS}" \
    || die "queue-burst scheduler stream did not close before normal TPU injection"

  local addr
  addr=$(wait_for_jito_tpu_quic_addr "${fd_log}" "${TIMEOUT_SECS}") \
    || die "jito-agave did not print a TPU QUIC address"
  local dst_ip="${addr%:*}"
  local dst_port="${addr##*:}"
  [[ -n "${dst_ip}" && "${dst_port}" =~ ^[0-9]+$ ]] \
    || die "could not parse jito TPU QUIC address: ${addr}"

  local payload
  payload=$(tr -d '[:space:]' < "${packet_path}")
  [[ -n "${payload}" ]] || die "normal TPU packet payload is empty"

  NORMAL_TPU_LOG="${normal_tpu_log}"
  NORMAL_TPU_PACKET="${packet_path}"
  NORMAL_TPU_DST="${addr}"
  NORMAL_TPU_PORT="${dst_port}"
  {
    printf 'normal_tpu_dst=%s\n' "${NORMAL_TPU_DST}"
    printf 'normal_tpu_packet_file=%s\n' "${packet_path}"
    printf 'normal_tpu_trigger=queue_reconnect\n'
    printf '\n'
  } >"${normal_tpu_log}"

  if ! "${FDDEV}" txn \
      --config "${FDDEV_TXN_CONFIG}" \
      --count 1 \
      --no-ready \
      --dst-ip "${dst_ip}" \
      --dst-port "${dst_port}" \
      --payload-base64-encoded "${payload}" \
      >>"${normal_tpu_log}" 2>&1; then
    echo "--- tail ${normal_tpu_log} ---" >&2
    tail -n 80 "${normal_tpu_log}" >&2 || true
    die "normal TPU queue-reconnect injection failed"
  fi
}

send_normal_tpu_packet_after_bam_disable() {
  local packet_path="$1"
  local bam_log="$2"
  local fd_log="$3"
  local operator_log="$4"
  local normal_tpu_log="$5"

  [[ -x "${FDDEV}" ]] || die "missing ${FDDEV}; build fddev before running BAM-disable TPU release scenarios"
  [[ -f "${FDDEV_TXN_CONFIG}" ]] || die "missing fddev txn config ${FDDEV_TXN_CONFIG}"
  wait_for_pattern "${operator_log}" '^operator: set_bam --disable' "${TIMEOUT_SECS}" \
    || die "Jito BAM disable did not start before normal TPU release injection"
  wait_for_pattern "${bam_log}" 'scheduler stream closed by validator conn=1' "${TIMEOUT_SECS}" \
    || die "Jito BAM scheduler stream did not close before normal TPU release injection"

  local addr
  addr=$(wait_for_jito_tpu_quic_addr "${fd_log}" "${TIMEOUT_SECS}") \
    || die "jito-agave did not print a TPU QUIC address"
  local dst_ip="${addr%:*}"
  local dst_port="${addr##*:}"
  [[ -n "${dst_ip}" && "${dst_port}" =~ ^[0-9]+$ ]] \
    || die "could not parse jito TPU QUIC address: ${addr}"

  local payload
  payload=$(tr -d '[:space:]' < "${packet_path}")
  [[ -n "${payload}" ]] || die "normal TPU packet payload is empty"

  NORMAL_TPU_LOG="${normal_tpu_log}"
  NORMAL_TPU_PACKET="${packet_path}"
  NORMAL_TPU_DST="${addr}"
  NORMAL_TPU_PORT="${dst_port}"
  NORMAL_TPU_EXPECTED_LANDED="true"
  {
    printf 'normal_tpu_dst=%s\n' "${NORMAL_TPU_DST}"
    printf 'normal_tpu_packet_file=%s\n' "${packet_path}"
    printf 'normal_tpu_trigger=bam_disabled\n'
    printf 'normal_tpu_expected_landed=true\n'
    printf '\n'
  } >"${normal_tpu_log}"

  if ! "${FDDEV}" txn \
      --config "${FDDEV_TXN_CONFIG}" \
      --count 1 \
      --no-ready \
      --dst-ip "${dst_ip}" \
      --dst-port "${dst_port}" \
      --payload-base64-encoded "${payload}" \
      >>"${normal_tpu_log}" 2>&1; then
    echo "--- tail ${normal_tpu_log} ---" >&2
    tail -n 80 "${normal_tpu_log}" >&2 || true
    die "normal TPU release injection failed"
  fi
}

run_bam_disabled_workload() {
  [[ -n "${BAM_DISABLED_WORKLOAD_SCRIPT}" ]] || return 0

  local close_pattern='scheduler stream closed by validator'
  local close_count
  close_count=$(grep -cE "${close_pattern}" "${BAM_LOG}" 2>/dev/null || true)

  local workload_log="${LOG_DIR}/broad-workload.log"
  {
    printf 'bam_disable_control=%s\n' "${AGAVE_VALIDATOR_BIN}"
    printf 'bam_close_count_before=%s\n' "${close_count}"
  } >"${workload_log}"
  if ! "${AGAVE_VALIDATOR_BIN}" --ledger "${LEDGER_DIR}" \
      set-bam-config --bam-url >>"${workload_log}" 2>&1; then
    die "failed to disable Jito BAM before post-result differential workload"
  fi

  local deadline=$((SECONDS + TIMEOUT_SECS))
  local observed_close_count=0
  while (( SECONDS < deadline )); do
    observed_close_count=$(grep -cE "${close_pattern}" "${BAM_LOG}" 2>/dev/null || true)
    (( observed_close_count >= close_count + 1 )) && break
    sleep 1
  done
  (( observed_close_count >= close_count + 1 )) \
    || die "Jito BAM scheduler stream did not close before post-result differential workload"
  sleep 1

  local payer_keypair="${LOG_DIR}/broad-workload-payer.json"
  local vote_keypair="${LOG_DIR}/broad-workload-vote.json"
  local payer_pubkey
  seeded_keypair_file 0 "${payer_keypair}" >"${LOG_DIR}/broad-workload-payer.out"
  seeded_keypair_file 700000 "${vote_keypair}" >"${LOG_DIR}/broad-workload-vote.out"
  payer_pubkey=$(solana-keygen pubkey "${payer_keypair}") \
    || die "failed to derive broad-workload payer"

  if ! env \
      RPC_URL="${RPC_URL}" \
      SOLANA_BIN_DIR="${VALIDATOR_BUILD_PATH}" \
      PAYER_KEYPAIR="${payer_keypair}" \
      VOTE_KEYPAIR="${vote_keypair}" \
      RUNNER_KIND="jito-agave" \
      TPU_PORT="${NORMAL_TPU_PORT}" \
      WORKLOAD_DIR="${LOG_DIR}/broad-workload" \
      "${BAM_DISABLED_WORKLOAD_SCRIPT}" \
      >>"${workload_log}" 2>&1; then
    echo "--- tail ${workload_log} ---" >&2
    tail -n 120 "${workload_log}" >&2 || true
    die "Jito broad differential workload failed"
  fi
}

make_keypair() {
  local path="$1"
  if [[ ! -f "${path}" ]]; then
    solana-keygen new --no-passphrase --silent --outfile "${path}" >/dev/null
  fi
}

ensure_bridge_bin() {
  if [[ ! -x "${BRIDGE_BIN}" || ! -x "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" || ! -x "${GEN_PROGRAM_INVOCATION_BIN}" || ! -x "${GEN_DURABLE_NONCE_TRANSFER_BIN}" || ! -x "${WRITE_SEEDED_KEYPAIR_BIN}" || ! -x "${MAKE_ALT_SETUP_TXNS_BIN}" ]]; then
    cargo build --quiet --manifest-path "${BRIDGE_MANIFEST}" --bins
  fi
}

bridge_raw_packet() {
  local input_path="$1"
  local output_path="$2"
  local bridge_out="$3"
  "${BRIDGE_BIN}" \
    --input "${input_path}" \
    --output "${output_path}" \
    --adapt-mode raw \
    >"${bridge_out}"
}

generate_raw_transfer_packet() {
  local output_txnctx="$1"
  local output_packet="$2"
  local gen_out="$3"
  local bridge_out="$4"
  local blockhash="$5"
  local to_seed_index="$6"
  local lamports="$7"
  local bogus_lookup="$8"
  local address_table_lookup="${9:-}"
  local cu_limit="${10:-300000}"
  local cu_price="${11:-}"

  local -a gen_args=(
    "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}"
    --output "${output_txnctx}"
    --system-kind transfer
    --transfer-count 1
    --lamports "${lamports}"
    --cu-limit "${cu_limit}"
    --recent-blockhash "${blockhash}"
    --from-seed-index 0
    --to-seed-index "${to_seed_index}"
  )
  if [[ "${bogus_lookup}" == "1" ]]; then
    gen_args+=(--bogus-address-table-lookup)
  fi
  if [[ -n "${address_table_lookup}" ]]; then
    gen_args+=(--address-table-lookup "${address_table_lookup}")
  fi
  if [[ -n "${cu_price}" ]]; then
    gen_args+=(--cu-price "${cu_price}")
  fi

  "${gen_args[@]}" >"${gen_out}"
  bridge_raw_packet "${output_txnctx}" "${output_packet}" "${bridge_out}"
}

seeded_keypair_file() {
  local seed_index="$1"
  local output_path="$2"
  "${WRITE_SEEDED_KEYPAIR_BIN}" \
    --seed-index "${seed_index}" \
    --output "${output_path}"
}

seeded_keypair_pubkey() {
  local seed_index="$1"
  local output_path="$2"
  local out
  out=$(seeded_keypair_file "${seed_index}" "${output_path}")
  printf '%s\n' "${out}" | sed -n 's/.* pubkey=\([^ ]*\) path=.*/\1/p'
}

setup_valid_alt_lookup() {
  local target_pubkey="$1"
  local payer_keypair="${LOG_DIR}/alt-payer.json"
  local setup_out="${LOG_DIR}/alt-setup.out"
  local recent_slot
  local recent_blockhash

  seeded_keypair_file 0 "${payer_keypair}" >"${LOG_DIR}/alt-payer.out"
  local payer_pubkey
  payer_pubkey=$(sed -n 's/.* pubkey=\([^ ]*\) path=.*/\1/p' "${LOG_DIR}/alt-payer.out")
  [[ -n "${payer_pubkey}" ]] || die "failed to parse valid ALT payer pubkey"

  prefund_if_needed "${payer_pubkey}" "${PAYER_PREFUND_LAMPORTS}"
  recent_slot=$(wait_rpc_finalized_slot_at_least 1 "${TIMEOUT_SECS}") \
    || die "validator finalized slot did not reach 1 for valid ALT setup; latest finalized=${recent_slot}"
  recent_blockhash=$(rpc_latest_blockhash)

  "${MAKE_ALT_SETUP_TXNS_BIN}" \
    --payer-keypair "${payer_keypair}" \
    --recent-slot "${recent_slot}" \
    --recent-blockhash "${recent_blockhash}" \
    --lookup-address "${target_pubkey}" \
    >"${setup_out}"

  local lookup_table
  local create_tx
  local extend_tx
  lookup_table=$(sed -n 's/^lookup_table=//p' "${setup_out}")
  create_tx=$(sed -n 's/^create_tx_base64=//p' "${setup_out}")
  extend_tx=$(sed -n 's/^extend_tx_base64=//p' "${setup_out}")
  [[ -n "${lookup_table}" && -n "${create_tx}" && -n "${extend_tx}" ]] \
    || die "failed to parse valid ALT setup transactions from ${setup_out}"

  local create_sig
  create_sig=$(rpc_send_base64_transaction "${create_tx}") \
    || die "failed to submit target valid ALT create transaction"
  printf '%s\n' "${create_sig}" >"${LOG_DIR}/alt-create.sig"
  wait_for_signature_ok "${create_sig}" "${TIMEOUT_SECS}" \
    || die "target valid ALT create transaction ${create_sig} did not land successfully"
  wait_for_account_present "${lookup_table}" "${TIMEOUT_SECS}" \
    || die "target lookup table ${lookup_table} was not created after create signature ${create_sig} landed"

  local extend_sig
  extend_sig=$(rpc_send_base64_transaction "${extend_tx}") \
    || die "failed to submit target valid ALT extend transaction"
  printf '%s\n' "${extend_sig}" >"${LOG_DIR}/alt-extend.sig"
  wait_for_signature_ok "${extend_sig}" "${TIMEOUT_SECS}" \
    || die "target valid ALT extend transaction ${extend_sig} did not land successfully"

  local extend_slot
  extend_slot=$(wait_for_signature_finalized "${extend_sig}" "${TIMEOUT_SECS}") \
    || die "target valid ALT extension transaction ${extend_sig} did not finalize"
  [[ "${extend_slot}" =~ ^[0-9]+$ ]] \
    || die "invalid target valid ALT extension slot: ${extend_slot}"
  wait_rpc_finalized_slot_at_least "${extend_slot}" "${TIMEOUT_SECS}" >/dev/null \
    || die "target valid ALT extension at slot ${extend_slot} did not become finalized"
  wait_for_account_present "${lookup_table}" "${TIMEOUT_SECS}" finalized \
    || die "target lookup table ${lookup_table} was absent from the finalized bank after extension slot ${extend_slot} rooted"

  rpc_signature_status "${create_sig}" >"${LOG_DIR}/alt-create.status.json" 2>/dev/null || true
  rpc_signature_status "${extend_sig}" >"${LOG_DIR}/alt-extend.status.json" 2>/dev/null || true
  rpc_account_info "${lookup_table}" >"${LOG_DIR}/alt-table.account.json" 2>/dev/null || true
  rpc_account_info "${lookup_table}" finalized >"${LOG_DIR}/alt-table.finalized.account.json" 2>/dev/null || true
  printf '%s\n' "${extend_slot}" >"${LOG_DIR}/alt-extend.slot"

  printf '%s\n' "${lookup_table}"
}

write_bad_signature_packet() {
  local input_packet_file="$1"
  local output_packet_file="$2"
  python3 - "${input_packet_file}" "${output_packet_file}" <<'PY'
import base64
import sys
from pathlib import Path

source = Path(sys.argv[1])
target = Path(sys.argv[2])
encoded = ""
for line in source.read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#"):
        encoded = line
        break
if not encoded:
    raise SystemExit(f"{source} did not contain a packet")

raw = bytearray(base64.b64decode(encoded, validate=True))
value = 0
shift = 0
cursor = None
for idx, byte in enumerate(raw[:5]):
    value |= (byte & 0x7F) << shift
    if byte & 0x80 == 0:
        cursor = idx + 1
        break
    shift += 7
if cursor is None or value < 1:
    raise SystemExit("packet does not contain a signature")
if len(raw) < cursor + 64:
    raise SystemExit("packet signature is truncated")
raw[cursor] ^= 0x01
target.write_text(base64.b64encode(raw).decode("ascii") + "\n")
PY
}

is_local_system_kind() {
  case "$1" in
    fee_only|transfer|assign|allocate|create_account|transfer_with_seed|create_account_with_seed)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

has_account_metadata_kind() {
  case "$1" in
    assign|allocate|create_account|create_account_with_seed)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
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
    owner)
      echo "${line}" | sed -n 's/.* owner=\([^ ]*\) system_ix=.*/\1/p'
      ;;
    space)
      echo "${line}" | sed -n 's/.* space=\([0-9][0-9]*\) owner=.*/\1/p'
      ;;
    *)
      die "unsupported bridge field ${field}"
      ;;
  esac
}

extract_gen_field() {
  local field="$1"
  local line="$2"
  case "${field}" in
    from)
      echo "${line}" | sed -n 's/.* from=\([^ ]*\) target=.*/\1/p'
      ;;
    target)
      echo "${line}" | sed -n 's/.* target=\([^ ]*\) total_lamports=.*/\1/p'
      ;;
    total_lamports)
      echo "${line}" | sed -n 's/.* total_lamports=\([0-9][0-9]*\) .*/\1/p'
      ;;
    *)
      die "unsupported generator field ${field}"
      ;;
  esac
}

prefund_if_needed() {
  local pubkey="$1"
  local desired="$2"
  if [[ -z "${pubkey}" || "${pubkey}" == "n/a" || ! "${desired}" =~ ^[0-9]+$ || "${desired}" == "0" ]]; then
    return 0
  fi

  local observed
  observed=$(rpc_balance "${pubkey}" 2>/dev/null || printf '0')
  if (( observed < desired )); then
    local airdrop_sig
    airdrop_sig=$(rpc_airdrop "${pubkey}" "$((desired - observed))" 2>/dev/null) \
      || die "airdrop failed for ${pubkey}: desired=${desired} observed=${observed}"
    [[ -n "${airdrop_sig}" ]] \
      || die "airdrop returned an empty signature for ${pubkey}: desired=${desired} observed=${observed}"
    wait_for_balance_at_least "${pubkey}" "${desired}" "${TIMEOUT_SECS}" >/dev/null \
      || die "account ${pubkey} did not reach prefund balance ${desired}"
  fi
}

prepare_target_durable_nonce_commit() {
  local packet_path="$1"
  local gen_out="$2"
  local lamports="$3"
  local seed_value="$4"
  local wrong_authority="${5:-0}"

  local payer_keypair="${LOG_DIR}/durable-nonce-payer.target.json"
  local nonce_keypair="${LOG_DIR}/durable-nonce-account.target.json"
  local nonce_seed=$((900000 + seed_value))
  local payer_pubkey
  local nonce_pubkey
  payer_pubkey=$(seeded_keypair_pubkey 0 "${payer_keypair}") \
    || die "failed to write target durable nonce payer keypair"
  nonce_pubkey=$(seeded_keypair_pubkey "${nonce_seed}" "${nonce_keypair}") \
    || die "failed to write target durable nonce account keypair"

  prefund_if_needed "${payer_pubkey}" "${PAYER_PREFUND_LAMPORTS}"

  wait_rpc_healthy "${TIMEOUT_SECS}" \
    || die "jito-agave RPC did not become healthy before durable nonce setup"
  solana --url "${RPC_URL}" \
    --keypair "${payer_keypair}" \
    create-nonce-account "${nonce_keypair}" 0.01 \
    --nonce-authority "${payer_pubkey}" \
    --commitment processed \
    --output json \
    >"${LOG_DIR}/durable-nonce-create.target.json" \
    2>"${LOG_DIR}/durable-nonce-create.target.err" \
    || die "failed to create target durable nonce account ${nonce_pubkey}; see ${LOG_DIR}/durable-nonce-create.target.err"

  wait_for_account_present "${nonce_pubkey}" "${TIMEOUT_SECS}" \
    || die "target durable nonce account ${nonce_pubkey} did not appear"
  local nonce_hash
  nonce_hash=$(wait_for_nonce_hash "${nonce_pubkey}" "${TIMEOUT_SECS}") \
    || die "target durable nonce account ${nonce_pubkey} did not expose a nonce hash"
  rpc_nonce_hash "${nonce_pubkey}" >"${LOG_DIR}/durable-nonce.target.hash"

  local nonce_authority_keypair="${payer_keypair}"
  if [[ "${wrong_authority}" == "1" ]]; then
    local wrong_authority_keypair="${LOG_DIR}/durable-nonce-wrong-authority.target.json"
    local wrong_authority_seed=$((910000 + seed_value))
    seeded_keypair_pubkey "${wrong_authority_seed}" "${wrong_authority_keypair}" >/dev/null \
      || die "failed to write target durable nonce wrong-authority keypair"
    nonce_authority_keypair="${wrong_authority_keypair}"
  fi

  "${GEN_DURABLE_NONCE_TRANSFER_BIN}" \
    --output "${packet_path}" \
    --nonce-hash "${nonce_hash}" \
    --nonce-keypair "${nonce_keypair}" \
    --nonce-authority-keypair "${nonce_authority_keypair}" \
    --from-keypair "${payer_keypair}" \
    --to-seed-index 1 \
    --lamports "${lamports}" \
    --cu-limit 300000 \
    >"${gen_out}"

  local gen_line
  gen_line=$(grep '^wrote durable nonce packet ' "${gen_out}" || true)
  [[ -n "${gen_line}" ]] || die "failed to capture target durable nonce generator summary"

  PAYER=$(printf '%s\n' "${gen_line}" | sed -n 's/.* from=\([^ ]*\) target=.*/\1/p')
  RECIPIENT_ONE=$(printf '%s\n' "${gen_line}" | sed -n 's/.* target=\([^ ]*\) nonce_account=.*/\1/p')
  RECIPIENT_TWO=""
  TARGET_BLOCKHASH="${nonce_hash}"
  TARGET_DURABLE_NONCE_ACCOUNT="${nonce_pubkey}"
  TARGET_DURABLE_NONCE_HASH="${nonce_hash}"
}

random_mixed_token_count() {
  local pattern="$1"
  local class="$2"
  local count=0
  local token
  local -a tokens=()
  local IFS=,
  read -r -a tokens <<< "${pattern}"
  for token in "${tokens[@]}"; do
    case "${class}:${token}" in
      committed:valid1|committed:valid2|\
      not_committed:replay1|not_committed:replay2|not_committed:empty|not_committed:stale1|not_committed:stale2|\
      stale:stale1|stale:stale2)
        count=$((count + 1))
        ;;
    esac
  done
  printf '%s\n' "${count}"
}

append_random_mixed_batches() {
  local scenario_path="$1"
  local pattern="$2"
  local packet_one="$3"
  local packet_two="$4"
  local seq_base="$5"

  local token packet max_schedule_slot seq_id idx=0
  local -a tokens=()
  local IFS=,
  read -r -a tokens <<< "${pattern}"
  for token in "${tokens[@]}"; do
    seq_id=$((seq_base + idx))
    case "${token}" in
      valid1|replay1)
        packet="${packet_one}"
        max_schedule_slot='"max"'
        ;;
      valid2|replay2)
        packet="${packet_two}"
        max_schedule_slot='"max"'
        ;;
      stale1)
        packet="${packet_one}"
        max_schedule_slot='0'
        ;;
      stale2)
        packet="${packet_two}"
        max_schedule_slot='0'
        ;;
      empty)
        packet=""
        max_schedule_slot='"max"'
        ;;
      *)
        die "unsupported random mixed token ${token}"
        ;;
    esac

    if [[ "${token}" == "empty" ]]; then
      cat >> "${scenario_path}" <<EOF2
[[events.batches]]
seq_id = ${seq_id}
packet_count = 0
max_schedule_slot = ${max_schedule_slot}
revert_on_error = true
simple_vote_tx = false

EOF2
    else
      cat >> "${scenario_path}" <<EOF2
[[events.batches]]
seq_id = ${seq_id}
packets_base64_file = "${packet}"
max_schedule_slot = ${max_schedule_slot}
revert_on_error = true
simple_vote_tx = false

EOF2
    fi
    idx=$((idx + 1))
  done
}

render_target_random_mixed_scenario() {
  local scenario_path="$1"
  local pattern="$2"
  local seq_id="$3"
  local packet_one="$4"
  local packet_two="$5"
  [[ -n "${pattern}" ]] || die "random_mixed_multi_batch source summary did not include random_mixed_pattern"

  local committed_count
  local not_committed_count
  committed_count=$(random_mixed_token_count "${pattern}" committed)
  not_committed_count=$(random_mixed_token_count "${pattern}" not_committed)

  cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=random_mixed_multi_batch pattern=${pattern}
description = "Target-specific replay for random_mixed_multi_batch: send a seed-selected mix of valid, replayed, empty, and stale regenerated System-program batches."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_multi_batch"

EOF2
  append_random_mixed_batches "${scenario_path}" "${pattern}" "${packet_one}" "${packet_two}" "${seq_id}"
  cat >> "${scenario_path}" <<EOF2
[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = ${committed_count}
timeout_ms = 45000

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = ${not_committed_count}
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
}

wait_for_random_mixed_results() {
  local pattern="$1"
  local seq_base="$2"
  [[ -n "${pattern}" ]] || die "random_mixed_multi_batch source summary did not include random_mixed_pattern"

  local committed_count
  local not_committed_count
  committed_count=$(random_mixed_token_count "${pattern}" committed)
  not_committed_count=$(random_mixed_token_count "${pattern}" not_committed)

  local token seq_id idx=0
  local -a tokens=()
  local IFS=,
  read -r -a tokens <<< "${pattern}"
  for token in "${tokens[@]}"; do
    seq_id=$((seq_base + idx))
    case "${token}" in
      valid1|valid2)
        wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${seq_id} status=committed txns=1" "${TIMEOUT_SECS}" \
          || die "jito-agave did not commit random-mixed ${token} seq_id=${seq_id}"
        ;;
      replay1|replay2)
        wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${seq_id} status=not_committed reason=transaction_error index=0 detail=ALREADY_PROCESSED" "${TIMEOUT_SECS}" \
          || die "jito-agave did not reject random-mixed ${token} seq_id=${seq_id} as ALREADY_PROCESSED"
        ;;
      empty)
        wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${seq_id} status=not_committed reason=deserialization_error index=0 detail=EMPTY" "${TIMEOUT_SECS}" \
          || die "jito-agave did not reject random-mixed empty batch seq_id=${seq_id}"
        ;;
      stale1|stale2)
        wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${seq_id} status=not_committed reason=scheduling_error detail=OUTSIDE_LEADER_SLOT" "${TIMEOUT_SECS}" \
          || die "jito-agave did not reject random-mixed stale batch seq_id=${seq_id}"
        ;;
      *)
        die "unsupported random mixed token ${token}"
        ;;
    esac
    idx=$((idx + 1))
  done

  wait_for_pattern "${BAM_LOG}" "scripted wait_inbound satisfied conn=1 kind=committed_batch observed=${committed_count}" "${TIMEOUT_SECS}" \
    || die "jito-agave did not observe the expected committed random-mixed count"
  wait_for_pattern "${BAM_LOG}" "scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=${not_committed_count}" "${TIMEOUT_SECS}" \
    || die "jito-agave did not observe the expected not_committed random-mixed count"
}

render_target_system_scenario() {
  local scenario_path="$1"
  local source_mode="$2"
  local seq_id="$3"
  local seq_two="$4"
  local packet_one="$5"
  local packet_two="$6"

  case "${source_mode}" in
    commit_once|seq_id_max_once|bam_fee_priority_commit|fee_only_commit|durable_nonce_commit)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: regenerate a locally signed packet with this validator's current blockhash."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    fee_only_reconnect|durable_nonce_reconnect)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: regenerate a local packet, close immediately, and require the committed result to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    durable_nonce_replay_after_reconnect)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: commit one regenerated durable-nonce packet, close conn=1, then replay it on conn=2."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 500

[[events]]
type = "send_batch"
seq_id = ${seq_two}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    durable_nonce_wrong_authority)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: regenerate a wrong-authority durable-nonce packet and require one not-committed result."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    durable_nonce_wrong_authority_reconnect)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: regenerate a wrong-authority durable-nonce packet, close immediately, and require the failure result to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bam_fee_priority_replay_after_reconnect)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: commit one regenerated priority-fee packet, close conn=1, then replay it on conn=2."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 500

[[events]]
type = "send_batch"
seq_id = ${seq_two}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bam_fee_url_churn_priority_commit|bam_fee_url_churn_same_slot_priority_commit)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: commit one priority-fee BAM packet under the first fee config, then idle while the harness switches to a second BAM URL/config."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 600000
EOF2
      ;;
    bam_fee_config_refresh_priority_commit|bam_fee_config_commission_refresh_priority_commit)
      refreshed_fee_recipient="${RECIPIENT_ONE}"
      refresh_description="Target-specific replay for ${source_mode}: commit one priority-fee BAM packet, mutate BamConfig on the same scheduler, wait for GetBuilderConfig refresh, then commit a second priority-fee BAM packet."
      if [[ "${source_mode}" == "bam_fee_config_commission_refresh_priority_commit" ]]; then
        refreshed_fee_recipient="${RECIPIENT_TWO}"
        refresh_description="Target-specific replay for ${source_mode}: commit one priority-fee BAM packet, change only BamConfig commission on the same scheduler, wait for GetBuilderConfig refresh, then commit a second priority-fee BAM packet."
      fi
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "${refresh_description}"
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "config_update"
prio_fee_recipient_pubkey = "${refreshed_fee_recipient}"
commission_bps = 700
include_block_engine_config = false

[[events]]
type = "sleep"
ms = 1500

[[events]]
type = "send_batch"
seq_id = ${seq_two}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 2
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    valid_alt_commit)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: commit a v0 transfer that references a pre-created address lookup table via BAM."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    invalid_alt_missing_table)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: reject a v0 transfer whose address lookup table account is missing."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    invalid_alt_missing_table_reconnect)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send a v0 transfer with a missing address lookup table, close immediately, and require one durable rejection."
heartbeat_interval_ms = 1000
resume_on_reconnect = true
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bam_cu_limit_fail)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: regenerate a low-compute-limit transfer and require a transaction-error BAM result."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bam_cu_limit_fail_reconnect)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: regenerate a low-compute-limit transfer, close immediately, and require the transaction-error BAM result to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    source_mix_bam_tpu|source_mix_duplicate_tpu_after_bam)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: commit one regenerated BAM packet, then keep BAM override active while the harness injects a direct TPU packet."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 5000
EOF2
      ;;
    disable_enable_tpu_release)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: commit one BAM packet, disable BAM so a direct TPU packet can land, then re-enable BAM and require reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 9000
EOF2
      ;;
    source_mix_precommit)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: keep BAM override active, inject a direct TPU packet before any BAM commit, then commit one regenerated BAM packet."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 5000

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    non_atomic_valid_multi_packet)
      local packet_batch="${scenario_path%.toml}.packets.txt"
      cat "${packet_one}" "${packet_two}" >"${packet_batch}"
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send two valid regenerated System-program packets in one revert_on_error=false BAM batch."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = false
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    non_atomic_single_packet)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send one regenerated System-program packet in a revert_on_error=false BAM batch."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = false
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    malformed_first_atomic)
      local packet_batch="${scenario_path%.toml}.packets.txt"
      {
        printf '%s\n' 'AA=='
        cat "${packet_two}"
      } >"${packet_batch}"
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send a malformed packet followed by a valid regenerated System-program packet in one revert_on_error=true BAM batch."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    malformed_first_atomic_reconnect)
      local packet_batch="${scenario_path%.toml}.packets.txt"
      {
        printf '%s\n' 'AA=='
        cat "${packet_two}"
      } >"${packet_batch}"
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send a malformed-first atomic BAM batch, close immediately, and require the terminal rejection to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    malformed_tail_atomic)
      local packet_batch="${scenario_path%.toml}.packets.txt"
      {
        cat "${packet_one}"
        printf '%s\n' 'AA=='
      } >"${packet_batch}"
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send a valid regenerated System-program packet followed by a malformed packet in one revert_on_error=true BAM batch."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    malformed_tail_atomic_reconnect)
      local packet_batch="${scenario_path%.toml}.packets.txt"
      {
        cat "${packet_one}"
        printf '%s\n' 'AA=='
      } >"${packet_batch}"
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send a valid regenerated System-program packet followed by a malformed packet, close immediately, and require the terminal rejection to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bad_signature_first_atomic)
      local packet_batch="${scenario_path%.toml}.packets.txt"
      local bad_signature_packet="${scenario_path%.toml}.bad_signature.txt"
      write_bad_signature_packet "${packet_one}" "${bad_signature_packet}"
      cat "${bad_signature_packet}" "${packet_two}" >"${packet_batch}"
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send a parse-valid bad-signature packet followed by a valid regenerated System-program packet in one revert_on_error=true BAM batch."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bad_signature_first_atomic_reconnect)
      local packet_batch="${scenario_path%.toml}.packets.txt"
      local bad_signature_packet="${scenario_path%.toml}.bad_signature.txt"
      write_bad_signature_packet "${packet_one}" "${bad_signature_packet}"
      cat "${bad_signature_packet}" "${packet_two}" >"${packet_batch}"
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send a parse-valid bad-signature packet followed by a valid regenerated System-program packet, close immediately, and require the terminal rejection to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bad_signature_tail_atomic)
      local packet_batch="${scenario_path%.toml}.packets.txt"
      local bad_signature_packet="${scenario_path%.toml}.bad_signature.txt"
      write_bad_signature_packet "${packet_two}" "${bad_signature_packet}"
      cat "${packet_one}" "${bad_signature_packet}" >"${packet_batch}"
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send a valid regenerated System-program packet followed by a parse-valid bad-signature packet in one revert_on_error=true BAM batch."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bad_signature_tail_atomic_reconnect)
      local packet_batch="${scenario_path%.toml}.packets.txt"
      local bad_signature_packet="${scenario_path%.toml}.bad_signature.txt"
      write_bad_signature_packet "${packet_two}" "${bad_signature_packet}"
      cat "${packet_one}" "${bad_signature_packet}" >"${packet_batch}"
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send a valid regenerated System-program packet followed by a parse-valid bad-signature packet, close immediately, and require the terminal rejection to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    stale_slot_reject)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send one locally valid System-program packet with max_schedule_slot=0 after the target has advanced past slot 0."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = 0
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    stale_slot_reject_reconnect)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send one locally valid System-program packet with max_schedule_slot=0, close immediately, reconnect, and require exactly one scheduling rejection."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = 0
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    empty_batch_reject)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send a zero-packet BAM batch and confirm it is rejected as EMPTY."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packet_count = 0
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    empty_batch_reject_reconnect)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send a zero-packet BAM batch, close immediately, reconnect, and require exactly one EMPTY rejection."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packet_count = 0
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    replay_same_conn)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: commit one regenerated System-program packet, then replay it on the same scheduler stream."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "send_batch"
seq_id = ${seq_two}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    replay_after_reconnect|seq_id_max_replay_after_reconnect)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: commit one regenerated System-program packet, close conn=1, then replay it on conn=2."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 500

[[events]]
type = "send_batch"
seq_id = ${seq_two}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    unique_after_reconnect|seq_id_wrap_sequence)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: commit one regenerated System-program packet, close conn=1, then commit a second unique packet on conn=2."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 500

[[events]]
type = "send_batch"
seq_id = ${seq_two}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    seq_collision_same_conn)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: commit one regenerated System-program packet, then send a different regenerated packet with the same seq_id on the same scheduler stream."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 2
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    seq_collision_reconnect)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: commit one regenerated System-program packet, reconnect, then send a different regenerated packet with the same seq_id."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 500

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    disable_enable_unique_after_reconnect|url_churn_unique_after_reconnect)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: commit one regenerated System-program packet, apply BAM control-plane churn, then commit a second unique packet after reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 3000

[[events]]
type = "send_batch"
seq_id = ${seq_two}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    mixed_multi_batch)
      local seq_mid=$((seq_id + 1))
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send valid, replayed, and second-valid regenerated System-program packets in one scheduler message."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_mid}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_two}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 2
timeout_ms = 45000

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    seq_id_out_of_order_multi_batch|seq_id_wrap_out_of_order_multi_batch)
      local revert_on_error="true"
      if [[ "${source_mode}" == "seq_id_wrap_out_of_order_multi_batch" ]]; then
        revert_on_error="false"
      fi
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send two valid regenerated System-program batches in descending seq_id order."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_two}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = ${revert_on_error}
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_id}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = ${revert_on_error}
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 2
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    seq_id_wrap_conflicting_spend_multi_batch)
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send two regenerated conflicting transfers across the u32 seq_id wrap boundary."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_two}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = false
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_id}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = false
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 2
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    mixed_empty_multi_batch)
      local seq_mid=$((seq_id + 1))
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send valid, empty, and second-valid regenerated System-program batches in one scheduler message."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_mid}
packet_count = 0
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_two}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 2
timeout_ms = 45000

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
	      ;;
	    mixed_malformed_multi_batch)
	      local seq_mid=$((seq_id + 1))
	      local malformed_packet="${scenario_path%.toml}.malformed.txt"
	      printf '%s\n' 'AA==' >"${malformed_packet}"
	      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send valid, malformed, and second-valid regenerated System-program batches in one scheduler message."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_mid}
packets_base64_file = "${malformed_packet}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_two}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 2
timeout_ms = 45000

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
	      ;;
	    mixed_bad_signature_multi_batch)
	      local seq_mid=$((seq_id + 1))
	      local bad_signature_packet="${scenario_path%.toml}.bad_signature.txt"
	      write_bad_signature_packet "${packet_one}" "${bad_signature_packet}"
	      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send valid, bad-signature, and second-valid regenerated System-program batches in one scheduler message."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_mid}
packets_base64_file = "${bad_signature_packet}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_two}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 2
timeout_ms = 45000

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
	      ;;
	    mixed_bad_signature_reconnect)
	      local seq_mid=$((seq_id + 1))
	      local bad_signature_packet="${scenario_path%.toml}.bad_signature.txt"
	      write_bad_signature_packet "${packet_one}" "${bad_signature_packet}"
	      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send valid, bad-signature, and second-valid regenerated System-program batches, then close after the bad-signature result."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_mid}
packets_base64_file = "${bad_signature_packet}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_two}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "close_stream"
EOF2
	      ;;
	    mixed_stale_multi_batch)
	      local seq_mid=$((seq_id + 1))
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send valid, stale, and second-valid regenerated System-program batches in one scheduler message."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_mid}
packets_base64_file = "${packet_one}"
max_schedule_slot = 0
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_two}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 2
timeout_ms = 45000

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    mixed_stale_reconnect)
      local seq_mid=$((seq_id + 1))
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send valid, stale, and second-valid regenerated System-program batches, then close after the stale result."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_mid}
packets_base64_file = "${packet_one}"
max_schedule_slot = 0
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_two}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "close_stream"
EOF2
      ;;
    mixed_terminal_producers_reconnect)
      local seq_malformed=$((seq_id + 1))
      local seq_stale=$((seq_id + 2))
      local seq_bad_sig=$((seq_id + 3))
      local malformed_packet="${scenario_path%.toml}.malformed.txt"
      local bad_signature_packet="${scenario_path%.toml}.bad_signature.txt"
      printf '%s\n' 'AA==' >"${malformed_packet}"
      write_bad_signature_packet "${packet_one}" "${bad_signature_packet}"
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send valid, malformed, stale, bad-signature, and second-valid regenerated System-program batches, then close after the first rejection."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_id}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_malformed}
packets_base64_file = "${malformed_packet}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_stale}
packets_base64_file = "${packet_one}"
max_schedule_slot = 0
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_bad_sig}
packets_base64_file = "${bad_signature_packet}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_two}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "close_stream"
EOF2
      ;;
    random_mixed_multi_batch)
      render_target_random_mixed_scenario \
        "${scenario_path}" \
        "${SOURCE_RANDOM_MIXED_PATTERN}" \
        "${seq_id}" \
        "${packet_one}" \
        "${packet_two}"
      ;;
    *)
      die "unsupported target System-program source mode ${source_mode}"
      ;;
  esac
}

write_max_schedule_slot_toml() {
  local value="$1"
  if [[ "${value}" =~ ^[0-9]+$ ]]; then
    printf 'max_schedule_slot = %s\n' "${value}"
  else
    printf 'max_schedule_slot = "%s"\n' "${value}"
  fi
}

render_target_fee_config_refresh_queue_scenario() {
  local scenario_path="$1"
  local seq_id="$2"
  local seq_two="$3"
  local old_config_packet="$4"
  local batch_count="$5"
  local close_after_results="$6"
  local max_schedule_slot="$7"
  local mode_name="${MODE_VALUE:-bam_fee_config_refresh_queue_burst}"
  local midqueue=0
  if [[ "${mode_name}" == "bam_fee_config_midqueue_refresh" || "${mode_name}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${mode_name}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
    midqueue=1
  fi
  local multi_reconnect=0
  if [[ "${mode_name}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
    multi_reconnect=1
  fi

  cat >"${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${mode_name}
description = "Target-specific replay for ${mode_name}: exercise priority-fee queue pressure across a live BamConfig refresh."
heartbeat_interval_ms = 1000
replay_on_reconnect = false
$( (( multi_reconnect )) && printf 'resume_on_reconnect = true\n' )

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${old_config_packet}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

EOF2

  local batch_idx
  local prefix_count="${batch_count}"
  local observe_count="${close_after_results}"
  if (( midqueue )); then
    prefix_count=$((batch_count / 2))
    (( prefix_count < 1 )) && prefix_count=1
    if ! [[ "${observe_count}" =~ ^[0-9]+$ ]] || (( observe_count < 1 )); then
      observe_count=1
    fi
    (( observe_count > prefix_count )) && observe_count="${prefix_count}"
  else
    prefix_count=0
  fi

  if (( midqueue )); then
    for batch_idx in $(seq 0 $((prefix_count - 1))); do
      local queue_seq=$((seq_two + batch_idx))
      local packet_file="${TARGET_QUEUE_PACKET_FILES[$batch_idx]}"
      [[ -n "${packet_file}" ]] || die "missing target queue packet for batch_idx=${batch_idx}"
      cat >>"${scenario_path}" <<EOF2
[[events]]
type = "send_batch"
seq_id = ${queue_seq}
packets_base64_file = "${packet_file}"
revert_on_error = true
simple_vote_tx = false

EOF2
      write_max_schedule_slot_toml "${max_schedule_slot}" >>"${scenario_path}"
      cat >>"${scenario_path}" <<EOF2

EOF2
    done

    cat >>"${scenario_path}" <<EOF2
[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = $((1 + observe_count))
timeout_ms = 45000

EOF2
  fi

  cat >>"${scenario_path}" <<EOF2
[[events]]
type = "config_update"
prio_fee_recipient_pubkey = "${RECIPIENT_ONE}"
commission_bps = 700
include_block_engine_config = false

[[events]]
type = "sleep"
ms = 1500

EOF2

  for batch_idx in $(seq "${prefix_count}" $((batch_count - 1))); do
    local queue_seq=$((seq_two + batch_idx))
    local packet_file="${TARGET_QUEUE_PACKET_FILES[$batch_idx]}"
    [[ -n "${packet_file}" ]] || die "missing target queue packet for batch_idx=${batch_idx}"
    cat >>"${scenario_path}" <<EOF2
[[events]]
type = "send_batch"
seq_id = ${queue_seq}
packets_base64_file = "${packet_file}"
revert_on_error = true
simple_vote_tx = false

EOF2
    write_max_schedule_slot_toml "${max_schedule_slot}" >>"${scenario_path}"
    cat >>"${scenario_path}" <<EOF2

EOF2
    if (( close_after_results > 0 && batch_idx + 1 == close_after_results )); then
      cat >>"${scenario_path}" <<EOF2
[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = $((1 + close_after_results))
timeout_ms = 45000

EOF2
    fi
  done

  if (( multi_reconnect )); then
    cat >>"${scenario_path}" <<EOF2
[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 2000

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 2000
EOF2
  else
    cat >>"${scenario_path}" <<EOF2
[[events]]
type = "close_stream"
EOF2
  fi
}

render_target_non_atomic_scenario() {
  local scenario_path="$1"
  local seq_id="$2"
  local packet_batch="$3"
  local source_mode="${4:-non_atomic_inconsistent_bundle}"
  local reconnect=0
  local wait_timeout_ms=45000
  if [[ "${source_mode}" == "non_atomic_partial_overdraft_reconnect" ]]; then
    reconnect=1
    wait_timeout_ms=5000
  fi

  cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send regenerated packets in one revert_on_error=false BAM batch."
heartbeat_interval_ms = 1000
replay_on_reconnect = false
$( (( reconnect )) && printf 'resume_on_reconnect = true\n' )

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = false
simple_vote_tx = false

EOF2

  if (( reconnect )); then
    cat >> "${scenario_path}" <<EOF2
[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

EOF2
  fi

  cat >> "${scenario_path}" <<EOF2
[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = ${wait_timeout_ms}

[[events]]
type = "sleep"
ms = 1000
EOF2
}

render_target_bam_fee_url_churn_second_scenario() {
  local scenario_path="$1"
  local seq_two="$2"
  local packet_two="$3"

  cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=bam_fee_url_churn_priority_commit secondary=true
description = "Target-specific replay for bam_fee_url_churn_priority_commit: send the second priority-fee packet after the validator reconnects to the alternate BAM URL/config."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_two}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
}

render_target_atomic_revert_scenario() {
  local scenario_path="$1"
  local seq_id="$2"
  local packet_batch="$3"
  local source_mode="${4:-atomic_revert}"

  if [[ "${source_mode}" == "source_mix_atomic_revert_precommit" ]]; then
    cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: keep BAM override active, inject a direct TPU packet before any BAM result, then reject an atomic BAM batch at index 1."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 5000

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
    return 0
  fi

  if [[ "${source_mode}" == "atomic_first_overdraft" || "${source_mode}" == "atomic_first_overdraft_reconnect" ]]; then
    if [[ "${source_mode}" == "atomic_first_overdraft_reconnect" ]]; then
      cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send two regenerated transfers where the first overdraws, close immediately, and require one terminal rejection across reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
      return 0
    fi

    cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send two regenerated transfers in one revert_on_error=true BAM batch where the first transfer overdraws and the suffix must not execute."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
    return 0
  fi

  if [[ "${source_mode}" == "atomic_revert_reconnect" ]]; then
    cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send an atomic-revert BAM batch, close immediately, and require the terminal rejection to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
    return 0
  fi

  cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=atomic_revert
description = "Target-specific replay for atomic_revert: send two regenerated transfers in one revert_on_error=true BAM batch where the second transfer overdraws the payer."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
}

render_target_atomic_mid_fail_scenario() {
  local scenario_path="$1"
  local seq_id="$2"
  local packet_batch="$3"
  local source_mode="${4:-atomic_mid_fail}"

  if [[ "${source_mode}" == "atomic_mid_fail_reconnect" ]]; then
    cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send valid, overdrawn, and valid regenerated transfers, close immediately, and require one terminal rejection across reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
    return 0
  fi

  cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=atomic_mid_fail
description = "Target-specific replay for atomic_mid_fail: send valid, overdrawn, and valid regenerated transfers in one revert_on_error=true BAM batch."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
}

render_target_atomic_resolver_mid_fail_scenario() {
  local scenario_path="$1"
  local seq_id="$2"
  local packet_batch="$3"
  local source_mode="${4:-atomic_resolver_mid_fail}"

  if [[ "${source_mode}" == "atomic_resolver_mid_fail_reconnect" ]]; then
    cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send valid, bogus-v0-lookup, and valid regenerated transfers, close immediately, and require one terminal rejection across reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
    return 0
  fi

  cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=atomic_resolver_mid_fail
description = "Target-specific replay for atomic_resolver_mid_fail: send valid, bogus-v0-lookup, and valid regenerated transfers in one revert_on_error=true BAM batch."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 10000

[[events]]
type = "sleep"
ms = 1000
EOF2
}

render_target_atomic_duplicate_sig_mid_fail_scenario() {
  local scenario_path="$1"
  local seq_id="$2"
  local packet_batch="$3"
  local source_mode="${4:-atomic_duplicate_sig_mid_fail}"

  if [[ "${source_mode}" == "atomic_duplicate_sig_mid_fail_reconnect" ]]; then
    cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for atomic_duplicate_sig_mid_fail_reconnect: send valid, duplicate-signature, and valid regenerated transfers in one atomic BAM batch, close immediately, and require one terminal rejection across reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
    return 0
  fi

  cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for atomic_duplicate_sig_mid_fail: send valid, duplicate-signature, and valid regenerated transfers in one revert_on_error=true BAM batch."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 10000

[[events]]
type = "sleep"
ms = 1000
EOF2
}

render_target_atomic_blockhash_mid_fail_scenario() {
  local scenario_path="$1"
  local seq_id="$2"
  local packet_batch="$3"
  local source_mode="${4:-atomic_blockhash_mid_fail}"

  if [[ "${source_mode}" == "atomic_blockhash_mid_fail_reconnect" ]]; then
    cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send fresh, expired-blockhash, and fresh transfers, close immediately, and require one terminal rejection across reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 200

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
    return 0
  fi

  cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=atomic_blockhash_mid_fail
description = "Target-specific replay for atomic_blockhash_mid_fail: send fresh, expired-blockhash, and fresh transfers in one revert_on_error=true BAM batch."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 15000

[[events]]
type = "sleep"
ms = 1000
EOF2
}

render_target_duplicate_seq_scenario() {
  local scenario_path="$1"
  local seq_id="$2"
  local packet_batch="$3"
  local source_mode="${4:-duplicate_seq_split}"

  if [[ "${source_mode}" == "duplicate_seq_split_reconnect" ]]; then
    local packet_first5="${packet_batch%.txt}.first5.txt"
    local packet_last="${packet_batch%.txt}.last1.txt"
    sed -n '1,5p' "${packet_batch}" >"${packet_first5}"
    sed -n '6p' "${packet_batch}" >"${packet_last}"
    [[ -s "${packet_first5}" && -s "${packet_last}" ]] \
      || die "failed to split target duplicate_seq_split_reconnect packet batch"
    cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${source_mode}
description = "Target-specific replay for ${source_mode}: send five regenerated transfers, reconnect, then send one regenerated transfer with the same seq_id."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_first5}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 45000

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 500

[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_last}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
    return 0
  fi

  cat > "${scenario_path}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=duplicate_seq_split
description = "Target-specific replay for duplicate_seq_split: send six regenerated transfers as a [5,1] same-seq_id split."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 100

[[events]]
type = "send_split_batch"
seq_id = ${seq_id}
splits = [5, 1]
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 2
timeout_ms = 45000

[[events]]
type = "sleep"
ms = 1000
EOF2
}

start_jito_disable_enable_operator() {
  [[ -x "${AGAVE_VALIDATOR_BIN}" ]] || die "missing ${AGAVE_VALIDATOR_BIN}; rerun with --build"
  (
    echo "operator: wait_log bam ${TIMEOUT_SECS}s scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed" "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for the first committed BAM batch"

    echo "operator: set_bam --disable"
    "${AGAVE_VALIDATOR_BIN}" --ledger "${LEDGER_DIR}" set-bam-config --bam-url

    echo "operator: wait_log bam ${TIMEOUT_SECS}s scheduler stream closed by validator conn=1"
    wait_for_pattern "${BAM_LOG}" 'scheduler stream closed by validator conn=1' "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for BAM stream closure after disable"

    echo "operator: set_bam --enable"
    "${AGAVE_VALIDATOR_BIN}" --ledger "${LEDGER_DIR}" set-bam-config --bam-url "${BAM_URL}"

    echo "operator: wait_log bam ${TIMEOUT_SECS}s InitSchedulerStream conn=2"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for BAM reconnect after enable"
    wait_for_pattern "${BAM_LOG}" 'scheduler<-validator auth proof .* conn=2' "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for conn=2 auth proof after enable"
  ) >"${OPERATOR_LOG}" 2>&1 &
  OPERATOR_PID=$!
}

start_jito_disable_enable_queue_operator() {
  [[ -x "${AGAVE_VALIDATOR_BIN}" ]] || die "missing ${AGAVE_VALIDATOR_BIN}; rerun with --build"
  (
    echo "operator: wait_log bam ${TIMEOUT_SECS}s scheduler<-validator batch_result seq_id=${SEQ_ONE}"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for the first queue BAM batch result"

    echo "operator: set_bam --disable"
    "${AGAVE_VALIDATOR_BIN}" --ledger "${LEDGER_DIR}" set-bam-config --bam-url

    echo "operator: wait_log bam ${TIMEOUT_SECS}s scheduler stream closed by validator conn=1"
    wait_for_pattern "${BAM_LOG}" 'scheduler stream closed by validator conn=1' "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for BAM stream closure after queue disable"

    echo "operator: set_bam --enable"
    "${AGAVE_VALIDATOR_BIN}" --ledger "${LEDGER_DIR}" set-bam-config --bam-url "${BAM_URL}"

    echo "operator: wait_log bam ${TIMEOUT_SECS}s InitSchedulerStream conn=2"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for BAM reconnect after queue enable"
    wait_for_pattern "${BAM_LOG}" 'scheduler<-validator auth proof .* conn=2' "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for conn=2 auth proof after queue enable"
  ) >"${OPERATOR_LOG}" 2>&1 &
  OPERATOR_PID=$!
}

start_jito_disable_enable_tpu_release_operator() {
  [[ -x "${AGAVE_VALIDATOR_BIN}" ]] || die "missing ${AGAVE_VALIDATOR_BIN}; rerun with --build"
  (
    echo "operator: wait_log bam ${TIMEOUT_SECS}s scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed" "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for the first committed BAM batch"

    echo "operator: set_bam --disable"
    "${AGAVE_VALIDATOR_BIN}" --ledger "${LEDGER_DIR}" set-bam-config --bam-url

    echo "operator: wait_log bam ${TIMEOUT_SECS}s scheduler stream closed by validator conn=1"
    wait_for_pattern "${BAM_LOG}" 'scheduler stream closed by validator conn=1' "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for BAM stream closure after disable"

    echo "operator: sleep_ms 6000"
    sleep 6

    echo "operator: set_bam --enable"
    "${AGAVE_VALIDATOR_BIN}" --ledger "${LEDGER_DIR}" set-bam-config --bam-url "${BAM_URL}"

    echo "operator: wait_log bam ${TIMEOUT_SECS}s InitSchedulerStream conn=2"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for BAM reconnect after enable"
    wait_for_pattern "${BAM_LOG}" 'scheduler<-validator auth proof .* conn=2' "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for conn=2 auth proof after enable"
  ) >"${OPERATOR_LOG}" 2>&1 &
  OPERATOR_PID=$!
}

start_jito_url_churn_operator() {
  [[ -x "${AGAVE_VALIDATOR_BIN}" ]] || die "missing ${AGAVE_VALIDATOR_BIN}; rerun with --build"
  (
    echo "operator: wait_log bam ${TIMEOUT_SECS}s scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed" "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for the first committed BAM batch"

    echo "operator: set_bam --url ${BAM_BAD_URL}"
    "${AGAVE_VALIDATOR_BIN}" --ledger "${LEDGER_DIR}" set-bam-config --bam-url "${BAM_BAD_URL}"

    echo "operator: wait_log bam ${TIMEOUT_SECS}s scheduler stream closed by validator conn=1"
    wait_for_pattern "${BAM_LOG}" 'scheduler stream closed by validator conn=1' "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for BAM stream closure after bad URL"

    echo "operator: set_bam --url ${BAM_URL}"
    "${AGAVE_VALIDATOR_BIN}" --ledger "${LEDGER_DIR}" set-bam-config --bam-url "${BAM_URL}"

    echo "operator: wait_log bam ${TIMEOUT_SECS}s InitSchedulerStream conn=2"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for BAM reconnect after URL restore"
    wait_for_pattern "${BAM_LOG}" 'scheduler<-validator auth proof .* conn=2' "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for conn=2 auth proof after URL restore"
  ) >"${OPERATOR_LOG}" 2>&1 &
  OPERATOR_PID=$!
}

url_bind_addr() {
  local url="$1"
  url="${url#http://}"
  url="${url#https://}"
  printf '%s\n' "${url%%/*}"
}

start_jito_fee_url_churn_operator() {
  [[ -x "${AGAVE_VALIDATOR_BIN}" ]] || die "missing ${AGAVE_VALIDATOR_BIN}; rerun with --build"
  (
    echo "operator: wait_log bam ${TIMEOUT_SECS}s scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed" "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for the first priority-fee BAM batch"

    echo "operator: set_bam --url ${BAM_BAD_URL}"
    "${AGAVE_VALIDATOR_BIN}" --ledger "${LEDGER_DIR}" set-bam-config --bam-url "${BAM_BAD_URL}"

    echo "operator: wait_log bam ${TIMEOUT_SECS}s scheduler stream closed by validator conn=1"
    wait_for_pattern "${BAM_LOG}" 'scheduler stream closed by validator conn=1' "${TIMEOUT_SECS}" \
      || die "jito operator timed out waiting for BAM stream closure after fee-config URL switch"
  ) >"${OPERATOR_LOG}" 2>&1 &
  OPERATOR_PID=$!
}

cleanup() {
  set +e
  if [[ -n "${OPERATOR_PID:-}" ]]; then
    kill "${OPERATOR_PID}" 2>/dev/null || true
    wait "${OPERATOR_PID}" 2>/dev/null || true
  fi
  if [[ -n "${BAM_PID:-}" ]]; then
    kill "${BAM_PID}" 2>/dev/null || true
    wait "${BAM_PID}" 2>/dev/null || true
  fi
  if [[ -n "${BAM_SECONDARY_PID:-}" ]]; then
    kill "${BAM_SECONDARY_PID}" 2>/dev/null || true
    wait "${BAM_SECONDARY_PID}" 2>/dev/null || true
  fi
  if [[ -n "${AGAVE_PID:-}" ]]; then
    kill "${AGAVE_PID}" 2>/dev/null || true
    wait "${AGAVE_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

KEY_DIR="${LOG_DIR}/keys"
LEDGER_LINK="${LOG_DIR}/ledger"
if (( ${#LEDGER_LINK} + 10 > 100 )); then
  SHORT_LEDGER_PARENT=$(mktemp -d /tmp/firebam-jito-ledger.XXXXXX)
  LEDGER_DIR="${SHORT_LEDGER_PARENT}/ledger"
  mkdir -p "${LEDGER_DIR}"
  ln -sfn "${LEDGER_DIR}" "${LEDGER_LINK}"
else
  LEDGER_DIR="${LEDGER_LINK}"
fi
mkdir -p "${KEY_DIR}" "${LEDGER_DIR}"

MINT_KEYPAIR="${KEY_DIR}/mint.json"
make_keypair "${MINT_KEYPAIR}"
MINT_PUBKEY=$(solana-keygen pubkey "${MINT_KEYPAIR}")

FD_LOG="${LOG_DIR}/fd.log"
BAM_LOG="${LOG_DIR}/bam.log"
BAM_SECONDARY_LOG="${LOG_DIR}/bam-secondary.log"
OPERATOR_LOG="${LOG_DIR}/operator.log"
: > "${OPERATOR_LOG}"

MODE_VALUE=$(summary_get mode)
INPUT_FAMILY_VALUE=$(summary_get input_family)
SEED_VALUE=$(summary_get seed)
SOURCE_INPUT_PATH=$(summary_get input_path)
SOURCE_INPUT_NOTE=$(summary_get input_note)
SOURCE_SYSTEM_KIND=$(summary_get system_kind)
SOURCE_RANDOM_MIXED_PATTERN=$(summary_get random_mixed_pattern)
SEQ_ONE=$(summary_get seq_one)
SEQ_TWO=$(summary_get seq_two)
SEQ_THREE=$(summary_get seq_three)
PAYER=$(summary_get payer)
RECIPIENT_ONE=$(summary_get recipient_one)
RECIPIENT_TWO=$(summary_get recipient_two)
SRC_RECIPIENT_ONE_INITIAL=$(summary_get recipient_one_initial)
SRC_RECIPIENT_ONE_EXPECTED=$(summary_get recipient_one_expected)
SRC_RECIPIENT_TWO_INITIAL=$(summary_get recipient_two_initial)
SRC_RECIPIENT_TWO_EXPECTED=$(summary_get recipient_two_expected)
RECIPIENT_ONE_OWNER_EXPECTED=$(summary_get recipient_one_owner_expected)
RECIPIENT_ONE_SPACE_EXPECTED=$(summary_get recipient_one_space_expected)
RECIPIENT_TWO_OWNER_EXPECTED=$(summary_get recipient_two_owner_expected)
RECIPIENT_TWO_SPACE_EXPECTED=$(summary_get recipient_two_space_expected)
EXTRA_PREFUND_KEYS=()
EXTRA_PREFUND_AMOUNTS=()

is_fee_url_churn_mode() {
  [[ "$1" == "bam_fee_url_churn_priority_commit" || "$1" == "bam_fee_url_churn_same_slot_priority_commit" ]]
}

is_fee_config_refresh_mode() {
  [[ "$1" == "bam_fee_config_refresh_priority_commit" || "$1" == "bam_fee_config_commission_refresh_priority_commit" || "$1" == "bam_fee_config_refresh_queue_burst" || "$1" == "bam_fee_config_refresh_source_mix_queue_burst" || "$1" == "bam_fee_config_midqueue_refresh" || "$1" == "bam_fee_config_midqueue_source_mix_queue_burst" || "$1" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]
}

is_fee_config_commission_only_mode() {
  [[ "$1" == "bam_fee_config_commission_refresh_priority_commit" ]]
}

is_fee_config_two_recipient_mode() {
  is_fee_url_churn_mode "$1" || { is_fee_config_refresh_mode "$1" && ! is_fee_config_commission_only_mode "$1"; }
}

is_fee_queue_burst_mode() {
  [[ "$1" == "bam_fee_queue_burst_reconnect" || "$1" == "bam_fee_source_mix_queue_burst_reconnect" || "$1" == "bam_fee_config_refresh_queue_burst" || "$1" == "bam_fee_config_refresh_source_mix_queue_burst" || "$1" == "bam_fee_config_midqueue_refresh" || "$1" == "bam_fee_config_midqueue_source_mix_queue_burst" || "$1" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]
}

is_queue_source_mix_mode() {
  [[ "$1" == "source_mix_queue_burst_reconnect" || "$1" == "source_mix_queue_burst_multi_reconnect" || "$1" == "bam_fee_source_mix_queue_burst_reconnect" || "$1" == "bam_fee_config_refresh_source_mix_queue_burst" || "$1" == "bam_fee_config_midqueue_source_mix_queue_burst" || "$1" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]
}

is_queue_multi_reconnect_mode() {
  [[ "$1" == "queue_burst_multi_reconnect" || "$1" == "source_mix_queue_burst_multi_reconnect" || "$1" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]
}

batch_result_slot_from_log() {
  local log_file="$1"
  local seq_id="$2"
  awk -v seq="${seq_id}" '
    /scheduler<-validator leader state slot=/ {
      if (match($0, /slot=[0-9]+/)) slot = substr($0, RSTART + 5, RLENGTH - 5)
    }
    $0 ~ "scheduler<-validator batch_result seq_id=" seq " " { print slot; exit }
  ' "${log_file}"
}

validator_supports_flag() {
  local flag="$1"
  "${SOLANA_TEST_VALIDATOR_BIN}" --help 2>&1 | grep -q -- "${flag}"
}

TEST_VALIDATOR_HAS_BAM_URL=0
AGAVE_ARGS=(
  "${SOLANA_TEST_VALIDATOR_BIN}"
  --ledger "${LEDGER_DIR}"
  --reset
  --rpc-port "${RPC_PORT}"
  --faucet-port "${FAUCET_PORT}"
  --mint "${MINT_PUBKEY}"
  --slots-per-epoch "${SLOTS_PER_EPOCH}"
  --limit-ledger-size "${LIMIT_LEDGER_SIZE}"
)
if validator_supports_flag "--bam-url"; then
  TEST_VALIDATOR_HAS_BAM_URL=1
  AGAVE_ARGS+=(--bam-url "${BAM_URL}")
fi
if validator_supports_flag "--enable-rpc-transaction-history"; then
  AGAVE_ARGS+=(--enable-rpc-transaction-history)
fi
if validator_supports_flag "--dynamic-port-range"; then
  dynamic_start=$((RPC_PORT + 200))
  dynamic_end=$((RPC_PORT + 260))
  if (( dynamic_end > 65535 )); then
    dynamic_start=1024
    dynamic_end=1084
  fi
  AGAVE_ARGS+=(--dynamic-port-range "${dynamic_start}-${dynamic_end}")
fi
if validator_supports_flag "--gossip-port"; then
  AGAVE_ARGS+=(--gossip-port "${GOSSIP_PORT}")
fi
if validator_supports_flag "--faucet-sol"; then
  AGAVE_ARGS+=(--faucet-sol 1000000000)
fi
if validator_supports_flag "--faucet-per-request-sol-cap"; then
  AGAVE_ARGS+=(--faucet-per-request-sol-cap 1000000000)
fi
if validator_supports_flag "--faucet-per-time-sol-cap"; then
  AGAVE_ARGS+=(--faucet-per-time-sol-cap 1000000000)
fi
if [[ -n "${TICKS_PER_SLOT}" ]]; then
  [[ "${TICKS_PER_SLOT}" =~ ^[0-9]+$ && "${TICKS_PER_SLOT}" -gt 0 ]] \
    || die "--ticks-per-slot must be a positive integer"
  validator_supports_flag "--ticks-per-slot" || die "solana-test-validator does not support --ticks-per-slot"
  AGAVE_ARGS+=(--ticks-per-slot "${TICKS_PER_SLOT}")
fi

"${AGAVE_ARGS[@]}" >"${FD_LOG}" 2>&1 &
AGAVE_PID=$!

wait_rpc_ready "${TIMEOUT_SECS}" || die "jito-agave RPC did not become ready at ${RPC_URL}"
kill -0 "${AGAVE_PID}" 2>/dev/null || die "jito-agave exited before startup completed; see ${FD_LOG}"
wait_for_pattern "${FD_LOG}" "^JSON RPC URL: ${RPC_URL_RE}$" "${TIMEOUT_SECS}" \
  || die "jito-agave did not print its JSON RPC URL at ${RPC_URL}"
wait_for_pattern "${FD_LOG}" '^TPU QUIC Address: ' "${TIMEOUT_SECS}" \
  || die "jito-agave did not print a TPU QUIC address"

EFFECTIVE_SCENARIO_FILE="${SCENARIO_FILE}"
TARGET_BLOCKHASH=""
TARGET_PACKET_FILE=""
TARGET_PACKET_TWO_FILE=""
TARGET_PACKET_THREE_FILE=""
TARGET_PACKET_BATCH_FILE=""
TARGET_BRIDGE_OUT=""
TARGET_BRIDGE_TWO_OUT=""
TARGET_BRIDGE_THREE_OUT=""
TARGET_NORMAL_TPU_PACKET_FILE=""
TARGET_NORMAL_TPU_BRIDGE_OUT=""
PARTIAL_DRAIN_LAST_SEQ=""
NORMAL_TPU_LOG=""
NORMAL_TPU_PACKET=""
NORMAL_TPU_DST=""
NORMAL_TPU_PORT=""
NORMAL_TPU_EXPECTED_LANDED=""
TARGET_DURABLE_NONCE_ACCOUNT=""
TARGET_DURABLE_NONCE_HASH=""
if [[ "${MODE_VALUE}" == "external_scenario" ]]; then
  ensure_bridge_bin

  EXTERNAL_RECIPE="${SOURCE_INPUT_PATH}"
  if [[ -z "${EXTERNAL_RECIPE}" || ! -f "${EXTERNAL_RECIPE}" ]]; then
    EXTERNAL_RECIPE="${SCENARIO_FILE}"
  fi
  [[ -f "${EXTERNAL_RECIPE}" ]] || die "missing external scenario recipe ${EXTERNAL_RECIPE}"

  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  EXTERNAL_MATERIALIZE_META="${LOG_DIR}/external-materialized.target.env"
  EXTERNAL_PACKET_DIR="${LOG_DIR}/external-packets.target"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"
  materialize_external_scenario \
    "${EXTERNAL_RECIPE}" \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${EXTERNAL_PACKET_DIR}" \
    "${EXTERNAL_MATERIALIZE_META}" \
    "${TARGET_BLOCKHASH}"
  TARGET_PACKET_FILE="${EXTERNAL_PACKET_DIR}"

  MATERIALIZED_PACKET_COUNT=$(metadata_get "${EXTERNAL_MATERIALIZE_META}" materialized_packet_count)
  PAYER=$(metadata_get "${EXTERNAL_MATERIALIZE_META}" payer)
  RECIPIENT_ONE=$(metadata_get "${EXTERNAL_MATERIALIZE_META}" first_recipient)
  RECIPIENT_TWO=$(metadata_get "${EXTERNAL_MATERIALIZE_META}" last_recipient)
  [[ -n "${PAYER}" ]] || PAYER="n/a"
  [[ -n "${RECIPIENT_ONE}" ]] || RECIPIENT_ONE="n/a"
  [[ -n "${RECIPIENT_TWO}" ]] || RECIPIENT_TWO="n/a"
  if [[ "${MATERIALIZED_PACKET_COUNT:-0}" =~ ^[0-9]+$ && "${MATERIALIZED_PACKET_COUNT:-0}" -gt 0 ]]; then
    SOURCE_SYSTEM_KIND="transfer"
  fi
elif [[ "${MODE_VALUE}" == "non_atomic_inconsistent_bundle" || "${MODE_VALUE}" == "non_atomic_first_overdraft" || "${MODE_VALUE}" == "non_atomic_partial_overdraft" || "${MODE_VALUE}" == "non_atomic_partial_overdraft_reconnect" || "${MODE_VALUE}" == "seq_id_wrap_conflicting_spend_multi_batch" ]]; then
  IFS=, read -r SOURCE_INPUT_ONE SOURCE_INPUT_TWO <<<"${SOURCE_INPUT_PATH}"
  [[ -n "${SOURCE_INPUT_ONE:-}" && -n "${SOURCE_INPUT_TWO:-}" ]] \
    || die "${MODE_VALUE} source summary did not contain two input paths"
  [[ -f "${SOURCE_INPUT_ONE}" ]] || die "missing first ${MODE_VALUE} source input ${SOURCE_INPUT_ONE}"
  [[ -f "${SOURCE_INPUT_TWO}" ]] || die "missing second ${MODE_VALUE} source input ${SOURCE_INPUT_TWO}"

	  ensure_bridge_bin

	  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  TARGET_PACKET_FILE="${LOG_DIR}/packet_one.target.txt"
  TARGET_PACKET_TWO_FILE="${LOG_DIR}/packet_two.target.txt"
  TARGET_PACKET_BATCH_FILE="${LOG_DIR}/packet_batch.target.txt"
  TARGET_BRIDGE_OUT="${LOG_DIR}/bridge_one.target.out"
  TARGET_BRIDGE_TWO_OUT="${LOG_DIR}/bridge_two.target.out"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_ONE}" \
    --output "${TARGET_PACKET_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 1 \
    >"${TARGET_BRIDGE_OUT}"

  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_TWO}" \
    --output "${TARGET_PACKET_TWO_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 2 \
    >"${TARGET_BRIDGE_TWO_OUT}"

  cat "${TARGET_PACKET_FILE}" "${TARGET_PACKET_TWO_FILE}" >"${TARGET_PACKET_BATCH_FILE}"

  TARGET_ADAPT=$(grep '^adapted family=' "${TARGET_BRIDGE_OUT}" || true)
  [[ -n "${TARGET_ADAPT}" ]] || die "failed to capture target adapted system summary"
  TARGET_ADAPT_TWO=$(grep '^adapted family=' "${TARGET_BRIDGE_TWO_OUT}" || true)
  [[ -n "${TARGET_ADAPT_TWO}" ]] || die "failed to capture second target adapted system summary"
  PAYER=$(extract_bridge_field from "${TARGET_ADAPT}")
  RECIPIENT_ONE=$(extract_bridge_field to "${TARGET_ADAPT}")
  RECIPIENT_TWO=$(extract_bridge_field to "${TARGET_ADAPT_TWO}")

  SOURCE_PRE_FROM=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*pre_from=\([0-9][0-9]*\).*/\1/p')
  if [[ "${SOURCE_PRE_FROM}" =~ ^[0-9]+$ ]]; then
    PAYER_PREFUND_LAMPORTS="${SOURCE_PRE_FROM}"
  fi

  if [[ "${MODE_VALUE}" == "non_atomic_inconsistent_bundle" || "${MODE_VALUE}" == "non_atomic_first_overdraft" || "${MODE_VALUE}" == "non_atomic_partial_overdraft" || "${MODE_VALUE}" == "non_atomic_partial_overdraft_reconnect" ]]; then
    render_target_non_atomic_scenario \
      "${EFFECTIVE_SCENARIO_FILE}" \
      "${SEQ_ONE:-1}" \
      "${TARGET_PACKET_BATCH_FILE}" \
      "${MODE_VALUE}"
  else
    render_target_system_scenario \
      "${EFFECTIVE_SCENARIO_FILE}" \
      "${MODE_VALUE}" \
      "${SEQ_ONE:-1}" \
      "${SEQ_TWO:-2}" \
      "${TARGET_PACKET_FILE}" \
      "${TARGET_PACKET_TWO_FILE}"
  fi
elif [[ "${MODE_VALUE}" == "non_atomic_mid_overdraft" ]]; then
  IFS=, read -r SOURCE_INPUT_ONE SOURCE_INPUT_TWO SOURCE_INPUT_THREE <<<"${SOURCE_INPUT_PATH}"
  [[ -n "${SOURCE_INPUT_ONE:-}" && -n "${SOURCE_INPUT_TWO:-}" && -n "${SOURCE_INPUT_THREE:-}" ]] \
    || die "non_atomic_mid_overdraft source summary did not contain three input paths"
  [[ -f "${SOURCE_INPUT_ONE}" ]] || die "missing first non_atomic_mid_overdraft source input ${SOURCE_INPUT_ONE}"
  [[ -f "${SOURCE_INPUT_TWO}" ]] || die "missing second non_atomic_mid_overdraft source input ${SOURCE_INPUT_TWO}"
  [[ -f "${SOURCE_INPUT_THREE}" ]] || die "missing third non_atomic_mid_overdraft source input ${SOURCE_INPUT_THREE}"

  ensure_bridge_bin

  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  TARGET_PACKET_FILE="${LOG_DIR}/packet_one.target.txt"
  TARGET_PACKET_TWO_FILE="${LOG_DIR}/packet_two.target.txt"
  TARGET_PACKET_THREE_FILE="${LOG_DIR}/packet_three.target.txt"
  TARGET_PACKET_BATCH_FILE="${LOG_DIR}/packet_batch.target.txt"
  TARGET_BRIDGE_OUT="${LOG_DIR}/bridge_one.target.out"
  TARGET_BRIDGE_TWO_OUT="${LOG_DIR}/bridge_two.target.out"
  TARGET_BRIDGE_THREE_OUT="${LOG_DIR}/bridge_three.target.out"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_ONE}" \
    --output "${TARGET_PACKET_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 1 \
    >"${TARGET_BRIDGE_OUT}"

  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_TWO}" \
    --output "${TARGET_PACKET_TWO_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 2 \
    >"${TARGET_BRIDGE_TWO_OUT}"

  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_THREE}" \
    --output "${TARGET_PACKET_THREE_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 1 \
    >"${TARGET_BRIDGE_THREE_OUT}"

  cat "${TARGET_PACKET_FILE}" "${TARGET_PACKET_TWO_FILE}" "${TARGET_PACKET_THREE_FILE}" >"${TARGET_PACKET_BATCH_FILE}"

  TARGET_ADAPT=$(grep '^adapted family=' "${TARGET_BRIDGE_OUT}" || true)
  [[ -n "${TARGET_ADAPT}" ]] || die "failed to capture first target non-atomic-mid system summary"
  TARGET_ADAPT_TWO=$(grep '^adapted family=' "${TARGET_BRIDGE_TWO_OUT}" || true)
  [[ -n "${TARGET_ADAPT_TWO}" ]] || die "failed to capture second target non-atomic-mid system summary"
  PAYER=$(extract_bridge_field from "${TARGET_ADAPT}")
  RECIPIENT_ONE=$(extract_bridge_field to "${TARGET_ADAPT}")
  RECIPIENT_TWO=$(extract_bridge_field to "${TARGET_ADAPT_TWO}")

  LAMPORTS_TWO=$(extract_bridge_field lamports "${TARGET_ADAPT_TWO}")
  [[ "${LAMPORTS_TWO}" =~ ^[0-9]+$ ]] || die "failed to parse failing non_atomic_mid_overdraft lamports"
  PAYER_PREFUND_LAMPORTS="${LAMPORTS_TWO}"

  render_target_non_atomic_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${SEQ_ONE:-1}" \
    "${TARGET_PACKET_BATCH_FILE}" \
    "${MODE_VALUE}"
elif [[ "${MODE_VALUE}" == "atomic_revert" || "${MODE_VALUE}" == "atomic_revert_reconnect" || "${MODE_VALUE}" == "atomic_first_overdraft" || "${MODE_VALUE}" == "atomic_first_overdraft_reconnect" || "${MODE_VALUE}" == "source_mix_atomic_revert_precommit" ]]; then
  SOURCE_ITER_DIR=$(cd -- "$(dirname -- "${SOURCE_SUMMARY}")" >/dev/null 2>&1 && pwd)
  SOURCE_INPUT_ONE="${SOURCE_ITER_DIR}/tx0.txnctx"
  SOURCE_INPUT_TWO="${SOURCE_ITER_DIR}/tx1.txnctx"
  [[ -f "${SOURCE_INPUT_ONE}" ]] || die "missing first atomic-revert source input ${SOURCE_INPUT_ONE}"
  [[ -f "${SOURCE_INPUT_TWO}" ]] || die "missing second atomic-revert source input ${SOURCE_INPUT_TWO}"

	  ensure_bridge_bin

	  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  TARGET_PACKET_FILE="${LOG_DIR}/packet_one.target.txt"
  TARGET_PACKET_TWO_FILE="${LOG_DIR}/packet_two.target.txt"
  TARGET_PACKET_BATCH_FILE="${LOG_DIR}/packet_batch.target.txt"
  TARGET_BRIDGE_OUT="${LOG_DIR}/bridge_one.target.out"
  TARGET_BRIDGE_TWO_OUT="${LOG_DIR}/bridge_two.target.out"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_ONE}" \
    --output "${TARGET_PACKET_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 1 \
    >"${TARGET_BRIDGE_OUT}"

  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_TWO}" \
    --output "${TARGET_PACKET_TWO_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 2 \
    >"${TARGET_BRIDGE_TWO_OUT}"

  cat "${TARGET_PACKET_FILE}" "${TARGET_PACKET_TWO_FILE}" >"${TARGET_PACKET_BATCH_FILE}"

  TARGET_ADAPT=$(grep '^adapted family=' "${TARGET_BRIDGE_OUT}" || true)
  [[ -n "${TARGET_ADAPT}" ]] || die "failed to capture target adapted system summary"
  TARGET_ADAPT_TWO=$(grep '^adapted family=' "${TARGET_BRIDGE_TWO_OUT}" || true)
  [[ -n "${TARGET_ADAPT_TWO}" ]] || die "failed to capture second target adapted system summary"
  PAYER=$(extract_bridge_field from "${TARGET_ADAPT}")
  RECIPIENT_ONE=$(extract_bridge_field to "${TARGET_ADAPT}")
  RECIPIENT_TWO=$(extract_bridge_field to "${TARGET_ADAPT_TWO}")

  LAMPORTS_ONE=$(extract_bridge_field lamports "${TARGET_ADAPT}")
  [[ "${LAMPORTS_ONE}" =~ ^[0-9]+$ ]] || die "failed to parse first atomic-revert lamports"
  if [[ "${MODE_VALUE}" == "atomic_first_overdraft" || "${MODE_VALUE}" == "atomic_first_overdraft_reconnect" ]]; then
    SOURCE_PRE_FROM=$(sed -n 's/.*pre_from=\([0-9][0-9]*\).*/\1/p' <<<"${SOURCE_INPUT_NOTE}")
    [[ "${SOURCE_PRE_FROM}" =~ ^[0-9]+$ ]] || die "atomic_first_overdraft source summary did not record pre_from"
    PAYER_PREFUND_LAMPORTS="${SOURCE_PRE_FROM}"
  else
    PAYER_PREFUND_LAMPORTS=$((LAMPORTS_ONE + 1000000000))
  fi

  render_target_atomic_revert_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${SEQ_ONE:-1}" \
    "${TARGET_PACKET_BATCH_FILE}" \
    "${MODE_VALUE}"
elif [[ "${MODE_VALUE}" == "atomic_mid_fail" || "${MODE_VALUE}" == "atomic_mid_fail_reconnect" ]]; then
  IFS=, read -r SOURCE_INPUT_ONE SOURCE_INPUT_TWO SOURCE_INPUT_THREE <<<"${SOURCE_INPUT_PATH}"
  [[ -n "${SOURCE_INPUT_ONE:-}" && -n "${SOURCE_INPUT_TWO:-}" && -n "${SOURCE_INPUT_THREE:-}" ]] \
    || die "atomic_mid_fail source summary did not contain three input paths"
  [[ -f "${SOURCE_INPUT_ONE}" ]] || die "missing first atomic-mid-fail source input ${SOURCE_INPUT_ONE}"
  [[ -f "${SOURCE_INPUT_TWO}" ]] || die "missing second atomic-mid-fail source input ${SOURCE_INPUT_TWO}"
  [[ -f "${SOURCE_INPUT_THREE}" ]] || die "missing third atomic-mid-fail source input ${SOURCE_INPUT_THREE}"

	  ensure_bridge_bin

	  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  TARGET_PACKET_FILE="${LOG_DIR}/packet_one.target.txt"
  TARGET_PACKET_TWO_FILE="${LOG_DIR}/packet_two.target.txt"
  TARGET_PACKET_THREE_FILE="${LOG_DIR}/packet_three.target.txt"
  TARGET_PACKET_BATCH_FILE="${LOG_DIR}/packet_batch.target.txt"
  TARGET_BRIDGE_OUT="${LOG_DIR}/bridge_one.target.out"
  TARGET_BRIDGE_TWO_OUT="${LOG_DIR}/bridge_two.target.out"
  TARGET_BRIDGE_THREE_OUT="${LOG_DIR}/bridge_three.target.out"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_ONE}" \
    --output "${TARGET_PACKET_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 1 \
    >"${TARGET_BRIDGE_OUT}"

  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_TWO}" \
    --output "${TARGET_PACKET_TWO_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 2 \
    >"${TARGET_BRIDGE_TWO_OUT}"

  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_THREE}" \
    --output "${TARGET_PACKET_THREE_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 1 \
    >"${TARGET_BRIDGE_THREE_OUT}"

  cat "${TARGET_PACKET_FILE}" "${TARGET_PACKET_TWO_FILE}" "${TARGET_PACKET_THREE_FILE}" >"${TARGET_PACKET_BATCH_FILE}"

  TARGET_ADAPT=$(grep '^adapted family=' "${TARGET_BRIDGE_OUT}" || true)
  [[ -n "${TARGET_ADAPT}" ]] || die "failed to capture first target atomic-mid-fail system summary"
  TARGET_ADAPT_TWO=$(grep '^adapted family=' "${TARGET_BRIDGE_TWO_OUT}" || true)
  [[ -n "${TARGET_ADAPT_TWO}" ]] || die "failed to capture second target atomic-mid-fail system summary"
  PAYER=$(extract_bridge_field from "${TARGET_ADAPT}")
  RECIPIENT_ONE=$(extract_bridge_field to "${TARGET_ADAPT}")
  RECIPIENT_TWO=$(extract_bridge_field to "${TARGET_ADAPT_TWO}")

  LAMPORTS_TWO=$(extract_bridge_field lamports "${TARGET_ADAPT_TWO}")
  [[ "${LAMPORTS_TWO}" =~ ^[0-9]+$ ]] || die "failed to parse failing atomic-mid-fail lamports"
  PAYER_PREFUND_LAMPORTS="${LAMPORTS_TWO}"

  render_target_atomic_mid_fail_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${SEQ_ONE:-1}" \
    "${TARGET_PACKET_BATCH_FILE}" \
    "${MODE_VALUE}"
elif [[ "${MODE_VALUE}" == "atomic_blockhash_mid_fail" || "${MODE_VALUE}" == "atomic_blockhash_mid_fail_reconnect" ]]; then
  ensure_bridge_bin

  TARGET_OLD_BLOCKHASH=$(rpc_latest_blockhash)
  TARGET_OLD_SLOT=$(rpc_slot 2>/dev/null || printf '0')
  TARGET_WAITED_SLOT=$(wait_rpc_slot_at_least "$((TARGET_OLD_SLOT + 170))" "${TIMEOUT_SECS}") \
    || die "jito-agave slot did not advance far enough to expire old blockhash; old_slot=${TARGET_OLD_SLOT} latest=${TARGET_WAITED_SLOT}"
  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  TARGET_PACKET_FILE="${LOG_DIR}/packet_one.target.txt"
  TARGET_PACKET_TWO_FILE="${LOG_DIR}/packet_two.target.txt"
  TARGET_PACKET_THREE_FILE="${LOG_DIR}/packet_three.target.txt"
  TARGET_PACKET_BATCH_FILE="${LOG_DIR}/packet_batch.target.txt"
  TARGET_BRIDGE_OUT="${LOG_DIR}/bridge_one.target.out"
  TARGET_BRIDGE_TWO_OUT="${LOG_DIR}/bridge_two.target.out"
  TARGET_BRIDGE_THREE_OUT="${LOG_DIR}/bridge_three.target.out"
  TARGET_TXNCTX_ONE="${LOG_DIR}/atomic-blockhash-tx0.target.txnctx"
  TARGET_TXNCTX_TWO="${LOG_DIR}/atomic-blockhash-tx1-expired.target.txnctx"
  TARGET_TXNCTX_THREE="${LOG_DIR}/atomic-blockhash-tx2.target.txnctx"
  TARGET_GEN_ONE_OUT="${LOG_DIR}/atomic-blockhash-tx0.target.gen.out"
  TARGET_GEN_TWO_OUT="${LOG_DIR}/atomic-blockhash-tx1.target.gen.out"
  TARGET_GEN_THREE_OUT="${LOG_DIR}/atomic-blockhash-tx2.target.gen.out"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  LAMPORTS_ONE=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*tx0_lamports=\([0-9][0-9]*\).*/\1/p')
  LAMPORTS_TWO=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*tx1_lamports=\([0-9][0-9]*\).*/\1/p')
  LAMPORTS_THREE=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*tx2_lamports=\([0-9][0-9]*\).*/\1/p')
  [[ "${LAMPORTS_ONE}" =~ ^[0-9]+$ ]] || LAMPORTS_ONE=1000000
  [[ "${LAMPORTS_TWO}" =~ ^[0-9]+$ ]] || LAMPORTS_TWO=1000001
  [[ "${LAMPORTS_THREE}" =~ ^[0-9]+$ ]] || LAMPORTS_THREE=1000002

  generate_raw_transfer_packet \
    "${TARGET_TXNCTX_ONE}" \
    "${TARGET_PACKET_FILE}" \
    "${TARGET_GEN_ONE_OUT}" \
    "${TARGET_BRIDGE_OUT}" \
    "${TARGET_BLOCKHASH}" \
    1 \
    "${LAMPORTS_ONE}" \
    0
  generate_raw_transfer_packet \
    "${TARGET_TXNCTX_TWO}" \
    "${TARGET_PACKET_TWO_FILE}" \
    "${TARGET_GEN_TWO_OUT}" \
    "${TARGET_BRIDGE_TWO_OUT}" \
    "${TARGET_OLD_BLOCKHASH}" \
    2 \
    "${LAMPORTS_TWO}" \
    0
  generate_raw_transfer_packet \
    "${TARGET_TXNCTX_THREE}" \
    "${TARGET_PACKET_THREE_FILE}" \
    "${TARGET_GEN_THREE_OUT}" \
    "${TARGET_BRIDGE_THREE_OUT}" \
    "${TARGET_BLOCKHASH}" \
    1 \
    "${LAMPORTS_THREE}" \
    0

  cat "${TARGET_PACKET_FILE}" "${TARGET_PACKET_TWO_FILE}" "${TARGET_PACKET_THREE_FILE}" >"${TARGET_PACKET_BATCH_FILE}"

  TARGET_GEN_LINE_ONE=$(grep '^wrote synthetic txnctx ' "${TARGET_GEN_ONE_OUT}" || true)
  TARGET_GEN_LINE_TWO=$(grep '^wrote synthetic txnctx ' "${TARGET_GEN_TWO_OUT}" || true)
  [[ -n "${TARGET_GEN_LINE_ONE}" && -n "${TARGET_GEN_LINE_TWO}" ]] \
    || die "failed to capture target atomic-blockhash generator summaries"
  PAYER=$(extract_gen_field from "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_ONE=$(extract_gen_field target "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_TWO=$(extract_gen_field target "${TARGET_GEN_LINE_TWO}")

  render_target_atomic_blockhash_mid_fail_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${SEQ_ONE:-1}" \
    "${TARGET_PACKET_BATCH_FILE}" \
    "${MODE_VALUE}"
elif [[ "${MODE_VALUE}" == "atomic_resolver_mid_fail" || "${MODE_VALUE}" == "atomic_resolver_mid_fail_reconnect" ]]; then
  ensure_bridge_bin

  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  TARGET_PACKET_FILE="${LOG_DIR}/packet_one.target.txt"
  TARGET_PACKET_TWO_FILE="${LOG_DIR}/packet_two.target.txt"
  TARGET_PACKET_THREE_FILE="${LOG_DIR}/packet_three.target.txt"
  TARGET_PACKET_BATCH_FILE="${LOG_DIR}/packet_batch.target.txt"
  TARGET_BRIDGE_OUT="${LOG_DIR}/bridge_one.target.out"
  TARGET_BRIDGE_TWO_OUT="${LOG_DIR}/bridge_two.target.out"
  TARGET_BRIDGE_THREE_OUT="${LOG_DIR}/bridge_three.target.out"
  TARGET_TXNCTX_ONE="${LOG_DIR}/atomic-resolver-tx0.target.txnctx"
  TARGET_TXNCTX_TWO="${LOG_DIR}/atomic-resolver-tx1-bogus-lookup.target.txnctx"
  TARGET_TXNCTX_THREE="${LOG_DIR}/atomic-resolver-tx2.target.txnctx"
  TARGET_GEN_ONE_OUT="${LOG_DIR}/atomic-resolver-tx0.target.gen.out"
  TARGET_GEN_TWO_OUT="${LOG_DIR}/atomic-resolver-tx1.target.gen.out"
  TARGET_GEN_THREE_OUT="${LOG_DIR}/atomic-resolver-tx2.target.gen.out"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  LAMPORTS_ONE=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*tx0_lamports=\([0-9][0-9]*\).*/\1/p')
  LAMPORTS_TWO=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*tx1_lamports=\([0-9][0-9]*\).*/\1/p')
  LAMPORTS_THREE=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*tx2_lamports=\([0-9][0-9]*\).*/\1/p')
  [[ "${LAMPORTS_ONE}" =~ ^[0-9]+$ ]] || LAMPORTS_ONE=1000000
  [[ "${LAMPORTS_TWO}" =~ ^[0-9]+$ ]] || LAMPORTS_TWO=1000001
  [[ "${LAMPORTS_THREE}" =~ ^[0-9]+$ ]] || LAMPORTS_THREE=1000002

  generate_raw_transfer_packet \
    "${TARGET_TXNCTX_ONE}" \
    "${TARGET_PACKET_FILE}" \
    "${TARGET_GEN_ONE_OUT}" \
    "${TARGET_BRIDGE_OUT}" \
    "${TARGET_BLOCKHASH}" \
    1 \
    "${LAMPORTS_ONE}" \
    0
  generate_raw_transfer_packet \
    "${TARGET_TXNCTX_TWO}" \
    "${TARGET_PACKET_TWO_FILE}" \
    "${TARGET_GEN_TWO_OUT}" \
    "${TARGET_BRIDGE_TWO_OUT}" \
    "${TARGET_BLOCKHASH}" \
    2 \
    "${LAMPORTS_TWO}" \
    1
  generate_raw_transfer_packet \
    "${TARGET_TXNCTX_THREE}" \
    "${TARGET_PACKET_THREE_FILE}" \
    "${TARGET_GEN_THREE_OUT}" \
    "${TARGET_BRIDGE_THREE_OUT}" \
    "${TARGET_BLOCKHASH}" \
    1 \
    "${LAMPORTS_THREE}" \
    0

  cat "${TARGET_PACKET_FILE}" "${TARGET_PACKET_TWO_FILE}" "${TARGET_PACKET_THREE_FILE}" >"${TARGET_PACKET_BATCH_FILE}"

  TARGET_GEN_LINE_ONE=$(grep '^wrote synthetic txnctx ' "${TARGET_GEN_ONE_OUT}" || true)
  TARGET_GEN_LINE_TWO=$(grep '^wrote synthetic txnctx ' "${TARGET_GEN_TWO_OUT}" || true)
  [[ -n "${TARGET_GEN_LINE_ONE}" && -n "${TARGET_GEN_LINE_TWO}" ]] \
    || die "failed to capture target atomic-resolver generator summaries"
  PAYER=$(extract_gen_field from "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_ONE=$(extract_gen_field target "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_TWO=$(extract_gen_field target "${TARGET_GEN_LINE_TWO}")

  render_target_atomic_resolver_mid_fail_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${SEQ_ONE:-1}" \
    "${TARGET_PACKET_BATCH_FILE}" \
    "${MODE_VALUE}"
elif [[ "${MODE_VALUE}" == "atomic_duplicate_sig_mid_fail" || "${MODE_VALUE}" == "atomic_duplicate_sig_mid_fail_reconnect" ]]; then
  ensure_bridge_bin

  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  TARGET_PACKET_FILE="${LOG_DIR}/packet_one.target.txt"
  TARGET_PACKET_TWO_FILE="${LOG_DIR}/packet_two.target.txt"
  TARGET_PACKET_THREE_FILE="${LOG_DIR}/packet_three.target.txt"
  TARGET_PACKET_BATCH_FILE="${LOG_DIR}/packet_batch.target.txt"
  TARGET_BRIDGE_OUT="${LOG_DIR}/bridge_one.target.out"
  TARGET_BRIDGE_TWO_OUT="${LOG_DIR}/bridge_two.target.out"
  TARGET_BRIDGE_THREE_OUT="${LOG_DIR}/bridge_three.target.out"
  TARGET_TXNCTX_ONE="${LOG_DIR}/atomic-duplicate-sig-tx0.target.txnctx"
  TARGET_TXNCTX_TWO="${LOG_DIR}/atomic-duplicate-sig-tx1-copy.target.txnctx"
  TARGET_TXNCTX_THREE="${LOG_DIR}/atomic-duplicate-sig-tx2.target.txnctx"
  TARGET_GEN_ONE_OUT="${LOG_DIR}/atomic-duplicate-sig-tx0.target.gen.out"
  TARGET_GEN_TWO_OUT="${LOG_DIR}/atomic-duplicate-sig-tx1.target.gen.out"
  TARGET_GEN_THREE_OUT="${LOG_DIR}/atomic-duplicate-sig-tx2.target.gen.out"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  LAMPORTS_ONE=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*tx0_lamports=\([0-9][0-9]*\).*/\1/p')
  LAMPORTS_THREE=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*tx2_lamports=\([0-9][0-9]*\).*/\1/p')
  [[ "${LAMPORTS_ONE}" =~ ^[0-9]+$ ]] || LAMPORTS_ONE=1000000
  [[ "${LAMPORTS_THREE}" =~ ^[0-9]+$ ]] || LAMPORTS_THREE=1000001

  generate_raw_transfer_packet \
    "${TARGET_TXNCTX_ONE}" \
    "${TARGET_PACKET_FILE}" \
    "${TARGET_GEN_ONE_OUT}" \
    "${TARGET_BRIDGE_OUT}" \
    "${TARGET_BLOCKHASH}" \
    1 \
    "${LAMPORTS_ONE}" \
    0
  cp "${TARGET_TXNCTX_ONE}" "${TARGET_TXNCTX_TWO}"
  cp "${TARGET_PACKET_FILE}" "${TARGET_PACKET_TWO_FILE}"
  cp "${TARGET_GEN_ONE_OUT}" "${TARGET_GEN_TWO_OUT}"
  cp "${TARGET_BRIDGE_OUT}" "${TARGET_BRIDGE_TWO_OUT}"
  generate_raw_transfer_packet \
    "${TARGET_TXNCTX_THREE}" \
    "${TARGET_PACKET_THREE_FILE}" \
    "${TARGET_GEN_THREE_OUT}" \
    "${TARGET_BRIDGE_THREE_OUT}" \
    "${TARGET_BLOCKHASH}" \
    2 \
    "${LAMPORTS_THREE}" \
    0

  cat "${TARGET_PACKET_FILE}" "${TARGET_PACKET_TWO_FILE}" "${TARGET_PACKET_THREE_FILE}" >"${TARGET_PACKET_BATCH_FILE}"

  TARGET_GEN_LINE_ONE=$(grep '^wrote synthetic txnctx ' "${TARGET_GEN_ONE_OUT}" || true)
  TARGET_GEN_LINE_THREE=$(grep '^wrote synthetic txnctx ' "${TARGET_GEN_THREE_OUT}" || true)
  [[ -n "${TARGET_GEN_LINE_ONE}" && -n "${TARGET_GEN_LINE_THREE}" ]] \
    || die "failed to capture target atomic-duplicate-signature generator summaries"
  PAYER=$(extract_gen_field from "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_ONE=$(extract_gen_field target "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_TWO=$(extract_gen_field target "${TARGET_GEN_LINE_THREE}")

  render_target_atomic_duplicate_sig_mid_fail_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${SEQ_ONE:-1}" \
    "${TARGET_PACKET_BATCH_FILE}" \
    "${MODE_VALUE}"
elif [[ "${MODE_VALUE}" == "non_atomic_partial_resolver_fail" || "${MODE_VALUE}" == "non_atomic_partial_blockhash_fail" || "${MODE_VALUE}" == "non_atomic_partial_duplicate_sig" || "${MODE_VALUE}" == "non_atomic_partial_cu_fail" ]]; then
  ensure_bridge_bin

  TARGET_PACKET_FILE="${LOG_DIR}/packet_one.target.txt"
  TARGET_PACKET_TWO_FILE="${LOG_DIR}/packet_two.target.txt"
  TARGET_PACKET_BATCH_FILE="${LOG_DIR}/packet_batch.target.txt"
  TARGET_BRIDGE_OUT="${LOG_DIR}/bridge_one.target.out"
  TARGET_BRIDGE_TWO_OUT="${LOG_DIR}/bridge_two.target.out"
  TARGET_TXNCTX_ONE="${LOG_DIR}/${MODE_VALUE}-tx0.target.txnctx"
  TARGET_TXNCTX_TWO="${LOG_DIR}/${MODE_VALUE}-tx1.target.txnctx"
  TARGET_GEN_ONE_OUT="${LOG_DIR}/${MODE_VALUE}-tx0.target.gen.out"
  TARGET_GEN_TWO_OUT="${LOG_DIR}/${MODE_VALUE}-tx1.target.gen.out"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  LAMPORTS_ONE=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*tx0_lamports=\([0-9][0-9]*\).*/\1/p')
  LAMPORTS_TWO=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*tx1_lamports=\([0-9][0-9]*\).*/\1/p')
  [[ "${LAMPORTS_ONE}" =~ ^[0-9]+$ ]] || LAMPORTS_ONE=1000000
  [[ "${LAMPORTS_TWO}" =~ ^[0-9]+$ ]] || LAMPORTS_TWO=1000001

  if [[ "${MODE_VALUE}" == "non_atomic_partial_blockhash_fail" ]]; then
    TARGET_OLD_BLOCKHASH=$(rpc_latest_blockhash)
    TARGET_OLD_SLOT=$(rpc_slot 2>/dev/null || printf '0')
    TARGET_WAITED_SLOT=$(wait_rpc_slot_at_least "$((TARGET_OLD_SLOT + 170))" "${TIMEOUT_SECS}") \
      || die "jito-agave slot did not advance far enough to expire old blockhash; old_slot=${TARGET_OLD_SLOT} latest=${TARGET_WAITED_SLOT}"
    TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  else
    TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  fi

  generate_raw_transfer_packet \
    "${TARGET_TXNCTX_ONE}" \
    "${TARGET_PACKET_FILE}" \
    "${TARGET_GEN_ONE_OUT}" \
    "${TARGET_BRIDGE_OUT}" \
    "${TARGET_BLOCKHASH}" \
    1 \
    "${LAMPORTS_ONE}" \
    0

  case "${MODE_VALUE}" in
    non_atomic_partial_resolver_fail)
      generate_raw_transfer_packet \
        "${TARGET_TXNCTX_TWO}" \
        "${TARGET_PACKET_TWO_FILE}" \
        "${TARGET_GEN_TWO_OUT}" \
        "${TARGET_BRIDGE_TWO_OUT}" \
        "${TARGET_BLOCKHASH}" \
        2 \
        "${LAMPORTS_TWO}" \
        1
      ;;
    non_atomic_partial_blockhash_fail)
      generate_raw_transfer_packet \
        "${TARGET_TXNCTX_TWO}" \
        "${TARGET_PACKET_TWO_FILE}" \
        "${TARGET_GEN_TWO_OUT}" \
        "${TARGET_BRIDGE_TWO_OUT}" \
        "${TARGET_OLD_BLOCKHASH}" \
        2 \
        "${LAMPORTS_TWO}" \
        0
      ;;
    non_atomic_partial_duplicate_sig)
      cp "${TARGET_TXNCTX_ONE}" "${TARGET_TXNCTX_TWO}"
      cp "${TARGET_PACKET_FILE}" "${TARGET_PACKET_TWO_FILE}"
      cp "${TARGET_GEN_ONE_OUT}" "${TARGET_GEN_TWO_OUT}"
      cp "${TARGET_BRIDGE_OUT}" "${TARGET_BRIDGE_TWO_OUT}"
      ;;
    non_atomic_partial_cu_fail)
      generate_raw_transfer_packet \
        "${TARGET_TXNCTX_TWO}" \
        "${TARGET_PACKET_TWO_FILE}" \
        "${TARGET_GEN_TWO_OUT}" \
        "${TARGET_BRIDGE_TWO_OUT}" \
        "${TARGET_BLOCKHASH}" \
        2 \
        "${LAMPORTS_TWO}" \
        0 \
        "" \
        1
      ;;
  esac

  cat "${TARGET_PACKET_FILE}" "${TARGET_PACKET_TWO_FILE}" >"${TARGET_PACKET_BATCH_FILE}"

  TARGET_GEN_LINE_ONE=$(grep '^wrote synthetic txnctx ' "${TARGET_GEN_ONE_OUT}" || true)
  TARGET_GEN_LINE_TWO=$(grep '^wrote synthetic txnctx ' "${TARGET_GEN_TWO_OUT}" || true)
  [[ -n "${TARGET_GEN_LINE_ONE}" && -n "${TARGET_GEN_LINE_TWO}" ]] \
    || die "failed to capture target non-atomic partial generator summaries"
  PAYER=$(extract_gen_field from "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_ONE=$(extract_gen_field target "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_TWO=$(extract_gen_field target "${TARGET_GEN_LINE_TWO}")

  render_target_non_atomic_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${SEQ_ONE:-1}" \
    "${TARGET_PACKET_BATCH_FILE}" \
    "${MODE_VALUE}"
elif [[ "${MODE_VALUE}" == "valid_alt_commit" ]]; then
  ensure_bridge_bin

  TARGET_PACKET_FILE="${LOG_DIR}/packet_one.target.txt"
  TARGET_BRIDGE_OUT="${LOG_DIR}/bridge_one.target.out"
  TARGET_TXNCTX_ONE="${LOG_DIR}/valid-alt.target.txnctx"
  TARGET_GEN_ONE_OUT="${LOG_DIR}/valid-alt.target.gen.out"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  LAMPORTS_ONE=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*lamports=\([0-9][0-9]*\).*/\1/p')
  [[ "${LAMPORTS_ONE}" =~ ^[0-9]+$ ]] || LAMPORTS_ONE=1000000

  TARGET_LOOKUP_KEYPAIR="${LOG_DIR}/alt-target-recipient.json"
  TARGET_LOOKUP_PUBKEY=$(seeded_keypair_pubkey 1 "${TARGET_LOOKUP_KEYPAIR}") \
    || die "failed to derive target valid-ALT lookup recipient"
  [[ -n "${TARGET_LOOKUP_PUBKEY}" ]] || die "empty target valid-ALT lookup recipient"

  TARGET_LOOKUP_TABLE=$(setup_valid_alt_lookup "${TARGET_LOOKUP_PUBKEY}") \
    || die "failed to set up target valid ALT lookup table"
  [[ -n "${TARGET_LOOKUP_TABLE}" ]] || die "empty target valid ALT lookup table"

  prefund_if_needed "${TARGET_LOOKUP_PUBKEY}" "${SRC_RECIPIENT_ONE_INITIAL}"
  TARGET_BLOCKHASH=$(rpc_latest_blockhash)

  generate_raw_transfer_packet \
    "${TARGET_TXNCTX_ONE}" \
    "${TARGET_PACKET_FILE}" \
    "${TARGET_GEN_ONE_OUT}" \
    "${TARGET_BRIDGE_OUT}" \
    "${TARGET_BLOCKHASH}" \
    1 \
    "${LAMPORTS_ONE}" \
    0 \
    "${TARGET_LOOKUP_TABLE}"

  rpc_simulate_base64_transaction_file "${TARGET_PACKET_FILE}" >"${LOG_DIR}/valid-alt.target.simulate.json" \
    || die "failed to simulate target valid-ALT transaction over RPC"
  if ! jq -e '(.error // null) == null and (.result.value.err // null) == null' \
      "${LOG_DIR}/valid-alt.target.simulate.json" >/dev/null 2>&1; then
    die "target valid-ALT transaction failed normal RPC simulation; see ${LOG_DIR}/valid-alt.target.simulate.json"
  fi

  TARGET_GEN_LINE_ONE=$(grep '^wrote synthetic txnctx ' "${TARGET_GEN_ONE_OUT}" || true)
  [[ -n "${TARGET_GEN_LINE_ONE}" ]] \
    || die "failed to capture target valid-ALT generator summary"
  PAYER=$(extract_gen_field from "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_ONE=$(extract_gen_field target "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_TWO=""

  render_target_system_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${MODE_VALUE}" \
    "${SEQ_ONE:-1}" \
    "${SEQ_TWO:-2}" \
    "${TARGET_PACKET_FILE}" \
    "${TARGET_PACKET_TWO_FILE}"
elif [[ "${MODE_VALUE}" == "invalid_alt_missing_table" || "${MODE_VALUE}" == "invalid_alt_missing_table_reconnect" ]]; then
  ensure_bridge_bin

  TARGET_PACKET_FILE="${LOG_DIR}/packet_one.target.txt"
  TARGET_BRIDGE_OUT="${LOG_DIR}/bridge_one.target.out"
  TARGET_TXNCTX_ONE="${LOG_DIR}/invalid-alt-missing-table.target.txnctx"
  TARGET_GEN_ONE_OUT="${LOG_DIR}/invalid-alt-missing-table.target.gen.out"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  LAMPORTS_ONE=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*lamports=\([0-9][0-9]*\).*/\1/p')
  [[ "${LAMPORTS_ONE}" =~ ^[0-9]+$ ]] || LAMPORTS_ONE=1000000

  TARGET_BLOCKHASH=$(rpc_latest_blockhash)

  generate_raw_transfer_packet \
    "${TARGET_TXNCTX_ONE}" \
    "${TARGET_PACKET_FILE}" \
    "${TARGET_GEN_ONE_OUT}" \
    "${TARGET_BRIDGE_OUT}" \
    "${TARGET_BLOCKHASH}" \
    1 \
    "${LAMPORTS_ONE}" \
    1

  TARGET_GEN_LINE_ONE=$(grep '^wrote synthetic txnctx ' "${TARGET_GEN_ONE_OUT}" || true)
  [[ -n "${TARGET_GEN_LINE_ONE}" ]] \
    || die "failed to capture target invalid-ALT generator summary"
  PAYER=$(extract_gen_field from "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_ONE=$(extract_gen_field target "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_TWO=""

  render_target_system_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${MODE_VALUE}" \
    "${SEQ_ONE:-1}" \
    "${SEQ_TWO:-2}" \
    "${TARGET_PACKET_FILE}" \
    "${TARGET_PACKET_TWO_FILE}"
elif [[ "${MODE_VALUE}" == "partial_drain_reconnect" || "${MODE_VALUE}" == "queue_burst_reconnect" || "${MODE_VALUE}" == "queue_burst64_reconnect" || "${MODE_VALUE}" == "queue_burst64_leader_plus1_reconnect" || "${MODE_VALUE}" == "schedule_boundary_jitter" || "${MODE_VALUE}" == "queue_reconnect_timing_jitter" || "${MODE_VALUE}" == "queue_burst_multi_reconnect" || "${MODE_VALUE}" == "source_mix_queue_burst_multi_reconnect" || "${MODE_VALUE}" == "queue_burst128_reconnect" || "${MODE_VALUE}" == "queue_burst256_reconnect" || "${MODE_VALUE}" == "queue_burst512_reconnect" || "${MODE_VALUE}" == "queue_burst_leader_reconnect" || "${MODE_VALUE}" == "queue_burst64_leader_reconnect" || "${MODE_VALUE}" == "bam_fee_queue_burst_reconnect" || "${MODE_VALUE}" == "bam_fee_source_mix_queue_burst_reconnect" || "${MODE_VALUE}" == "source_mix_queue_burst_reconnect" || "${MODE_VALUE}" == "disable_enable_queue_burst_reconnect" ]]; then
  SOURCE_ITER_DIR=$(cd -- "$(dirname -- "${SOURCE_SUMMARY}")" >/dev/null 2>&1 && pwd)
  SOURCE_INPUT_ONE="${SOURCE_ITER_DIR}/input.txnctx"
  [[ -f "${SOURCE_INPUT_ONE}" ]] || die "missing partial-drain source input ${SOURCE_INPUT_ONE}"

  BATCH_COUNT=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*batch_count=\([0-9][0-9]*\).*/\1/p')
  if [[ ! "${BATCH_COUNT}" =~ ^[0-9]+$ ]]; then
    shopt -s nullglob
    SOURCE_PARTIAL_PACKETS=("${SOURCE_ITER_DIR}"/packet_*.txt)
    shopt -u nullglob
    BATCH_COUNT=${#SOURCE_PARTIAL_PACKETS[@]}
  fi
  [[ "${BATCH_COUNT}" =~ ^[0-9]+$ && "${BATCH_COUNT}" -gt 1 ]] \
    || die "failed to derive partial-drain batch count"
	  PARTIAL_DRAIN_LAST_SEQ=$((200 + BATCH_COUNT - 1))
	  MAX_SCHEDULE_SLOT_VALUE=max
  if [[ "${MODE_VALUE}" == "queue_burst_leader_reconnect" || "${MODE_VALUE}" == "queue_burst64_leader_reconnect" ]]; then
    MAX_SCHEDULE_SLOT_VALUE=leader
  elif [[ "${MODE_VALUE}" == "queue_burst64_leader_plus1_reconnect" ]]; then
    MAX_SCHEDULE_SLOT_VALUE=leader+1
  fi
  SOURCE_MAX_SCHEDULE_SLOT=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*max_schedule_slot=\([^ ]*\).*/\1/p')
	  if [[ -n "${SOURCE_MAX_SCHEDULE_SLOT}" ]]; then
	    MAX_SCHEDULE_SLOT_VALUE="${SOURCE_MAX_SCHEDULE_SLOT}"
	  fi
	  CLOSE_AFTER_RESULTS=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*close_after_results=\([0-9][0-9]*\).*/\1/p')
	  if [[ -z "${CLOSE_AFTER_RESULTS}" ]]; then
	    CLOSE_AFTER_RESULTS=1
	  fi
		  [[ "${CLOSE_AFTER_RESULTS}" =~ ^[0-9]+$ ]] || die "failed to derive partial-drain close_after_results"
		  (( CLOSE_AFTER_RESULTS <= BATCH_COUNT )) || die "partial-drain close_after_results exceeds batch count"
		  SECOND_CLOSE_AFTER_RESULTS=0
		  if is_queue_multi_reconnect_mode "${MODE_VALUE}"; then
		    SECOND_CLOSE_AFTER_RESULTS=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*second_close_after_results=\([0-9][0-9]*\).*/\1/p')
		    if [[ -z "${SECOND_CLOSE_AFTER_RESULTS}" ]]; then
		      SECOND_CLOSE_AFTER_RESULTS=1
		    fi
		    [[ "${SECOND_CLOSE_AFTER_RESULTS}" =~ ^[0-9]+$ ]] || die "failed to derive ${MODE_VALUE} second_close_after_results"
		    (( SECOND_CLOSE_AFTER_RESULTS <= BATCH_COUNT )) || die "${MODE_VALUE} second_close_after_results exceeds batch count"
		  fi

		  ensure_bridge_bin
	  if is_queue_source_mix_mode "${MODE_VALUE}"; then
	    TARGET_NORMAL_TPU_PACKET_FILE="${LOG_DIR}/normal_tpu.target.txt"
	    TARGET_NORMAL_TPU_BRIDGE_OUT="${LOG_DIR}/normal_tpu.target.bridge.out"
	    TARGET_NORMAL_TPU_TO_INDEX=$((1 + BATCH_COUNT + 1))
	  fi

	  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  cat >"${EFFECTIVE_SCENARIO_FILE}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${MODE_VALUE}
description = "Target-specific replay for ${MODE_VALUE}: queue multiple regenerated transfers, trigger the configured reconnect path after the first result, and observe later drained results."
heartbeat_interval_ms = 1000
replay_on_reconnect = false
$(is_queue_multi_reconnect_mode "${MODE_VALUE}" && printf 'resume_on_reconnect = true\n')

[[events]]
type = "sleep"
ms = 100

EOF2

  first_adapt=""
  partial_recipients=()
  for batch_idx in $(seq 0 $((BATCH_COUNT - 1))); do
    seq_id=$((200 + batch_idx))
    packet_path="${LOG_DIR}/packet_${seq_id}.target.txt"
    bridge_out="${LOG_DIR}/bridge_${seq_id}.target.out"

    "${BRIDGE_BIN}" \
      --input "${SOURCE_INPUT_ONE}" \
      --output "${packet_path}" \
      --adapt-mode local-system-transfer \
      --recent-blockhash "${TARGET_BLOCKHASH}" \
      --from-genesis-account 0 \
      --to-genesis-account $((1 + batch_idx)) \
      >"${bridge_out}"

    adapt_line=$(grep '^adapted family=' "${bridge_out}" || true)
    [[ -n "${adapt_line}" ]] || die "failed to capture partial-drain adapted summary for seq_id=${seq_id}"
    if [[ -z "${first_adapt}" ]]; then
      first_adapt="${adapt_line}"
    fi
    recipient=$(extract_bridge_field to "${adapt_line}")
    partial_recipients+=("${recipient}")
    EXTRA_PREFUND_KEYS+=("${recipient}")
    EXTRA_PREFUND_AMOUNTS+=("1000000000")

    cat >>"${EFFECTIVE_SCENARIO_FILE}" <<EOF2
[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_path}"
max_schedule_slot = "${MAX_SCHEDULE_SLOT_VALUE}"
revert_on_error = true
simple_vote_tx = false

EOF2

	    if (( CLOSE_AFTER_RESULTS > 0 && batch_idx + 1 == CLOSE_AFTER_RESULTS )); then
	      cat >>"${EFFECTIVE_SCENARIO_FILE}" <<EOF2
[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = ${CLOSE_AFTER_RESULTS}
timeout_ms = 45000

EOF2
	    fi
	  done
	  if is_queue_source_mix_mode "${MODE_VALUE}"; then
	    "${BRIDGE_BIN}" \
	      --input "${SOURCE_INPUT_ONE}" \
	      --output "${TARGET_NORMAL_TPU_PACKET_FILE}" \
	      --adapt-mode local-system-transfer \
	      --recent-blockhash "${TARGET_BLOCKHASH}" \
	      --from-genesis-account 0 \
	      --to-genesis-account "${TARGET_NORMAL_TPU_TO_INDEX}" \
	      >"${TARGET_NORMAL_TPU_BRIDGE_OUT}"
	    normal_tpu_adapt=$(grep '^adapted family=' "${TARGET_NORMAL_TPU_BRIDGE_OUT}" || true)
	    [[ -n "${normal_tpu_adapt}" ]] || die "failed to capture normal TPU adapted summary for ${MODE_VALUE}"
	    normal_tpu_recipient=$(extract_bridge_field to "${normal_tpu_adapt}")
	    EXTRA_PREFUND_KEYS+=("${normal_tpu_recipient}")
	    EXTRA_PREFUND_AMOUNTS+=("1000000000")
	  fi

		  if [[ "${MODE_VALUE}" == "disable_enable_queue_burst_reconnect" ]]; then
		    cat >>"${EFFECTIVE_SCENARIO_FILE}" <<EOF2
[[events]]
type = "sleep"
ms = 600000
EOF2
		  elif is_queue_multi_reconnect_mode "${MODE_VALUE}"; then
		    cat >>"${EFFECTIVE_SCENARIO_FILE}" <<EOF2
[[events]]
type = "close_stream"

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = ${SECOND_CLOSE_AFTER_RESULTS}
timeout_ms = 45000

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 2000
EOF2
		  else
		    cat >>"${EFFECTIVE_SCENARIO_FILE}" <<EOF2
[[events]]
type = "close_stream"
EOF2
	  fi

  [[ -n "${first_adapt}" ]] || die "failed to capture first partial-drain adapted system summary"
  PAYER=$(extract_bridge_field from "${first_adapt}")
  RECIPIENT_ONE=""
  RECIPIENT_TWO=""
  if is_fee_queue_burst_mode "${MODE_VALUE}"; then
    (( ${#partial_recipients[@]} >= 2 )) || die "fee queue burst requires at least two target recipients"
    RECIPIENT_ONE="${partial_recipients[0]}"
    RECIPIENT_TWO="${partial_recipients[1]}"
  fi

	  prefund_if_needed "${PAYER}" "${PAYER_PREFUND_LAMPORTS}"
	  for recipient in "${partial_recipients[@]}"; do
	    prefund_if_needed "${recipient}" "1000000000"
	  done
	  for idx in "${!EXTRA_PREFUND_KEYS[@]}"; do
	    prefund_if_needed "${EXTRA_PREFUND_KEYS[$idx]}" "${EXTRA_PREFUND_AMOUNTS[$idx]}"
	  done
	  EXTRA_PREFUND_KEYS=()
	  EXTRA_PREFUND_AMOUNTS=()

  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  cat >"${EFFECTIVE_SCENARIO_FILE}" <<EOF2
# regenerated_for_target=${TARGET_NAME} source_mode=${MODE_VALUE}
description = "Target-specific replay for ${MODE_VALUE}: queue multiple regenerated transfers with a post-prefund blockhash, trigger the configured reconnect path after the first result, and observe later drained results."
heartbeat_interval_ms = 1000
replay_on_reconnect = false
$(is_queue_multi_reconnect_mode "${MODE_VALUE}" && printf 'resume_on_reconnect = true\n')

[[events]]
type = "sleep"
ms = 100

EOF2

  for batch_idx in $(seq 0 $((BATCH_COUNT - 1))); do
    seq_id=$((200 + batch_idx))
    packet_path="${LOG_DIR}/packet_${seq_id}.target.txt"
    bridge_out="${LOG_DIR}/bridge_${seq_id}.target.out"

    "${BRIDGE_BIN}" \
      --input "${SOURCE_INPUT_ONE}" \
      --output "${packet_path}" \
      --adapt-mode local-system-transfer \
      --recent-blockhash "${TARGET_BLOCKHASH}" \
      --from-genesis-account 0 \
      --to-genesis-account $((1 + batch_idx)) \
      >"${bridge_out}"

    cat >>"${EFFECTIVE_SCENARIO_FILE}" <<EOF2
[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_path}"
max_schedule_slot = "${MAX_SCHEDULE_SLOT_VALUE}"
revert_on_error = true
simple_vote_tx = false

EOF2

	    if (( CLOSE_AFTER_RESULTS > 0 && batch_idx + 1 == CLOSE_AFTER_RESULTS )); then
	      cat >>"${EFFECTIVE_SCENARIO_FILE}" <<EOF2
[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = ${CLOSE_AFTER_RESULTS}
timeout_ms = 45000

EOF2
	    fi
	  done
	  if is_queue_source_mix_mode "${MODE_VALUE}"; then
	    "${BRIDGE_BIN}" \
	      --input "${SOURCE_INPUT_ONE}" \
	      --output "${TARGET_NORMAL_TPU_PACKET_FILE}" \
	      --adapt-mode local-system-transfer \
	      --recent-blockhash "${TARGET_BLOCKHASH}" \
	      --from-genesis-account 0 \
	      --to-genesis-account "${TARGET_NORMAL_TPU_TO_INDEX}" \
	      >"${TARGET_NORMAL_TPU_BRIDGE_OUT}"
	  fi

		  if [[ "${MODE_VALUE}" == "disable_enable_queue_burst_reconnect" ]]; then
		    cat >>"${EFFECTIVE_SCENARIO_FILE}" <<EOF2
[[events]]
type = "sleep"
ms = 600000
EOF2
		  elif is_queue_multi_reconnect_mode "${MODE_VALUE}"; then
		    cat >>"${EFFECTIVE_SCENARIO_FILE}" <<EOF2
[[events]]
type = "close_stream"

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = ${SECOND_CLOSE_AFTER_RESULTS}
timeout_ms = 45000

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 2000
EOF2
		  else
		    cat >>"${EFFECTIVE_SCENARIO_FILE}" <<EOF2
[[events]]
type = "close_stream"
EOF2
	  fi
elif [[ "${MODE_VALUE}" == "duplicate_seq_split" || "${MODE_VALUE}" == "duplicate_seq_split_reconnect" ]]; then
  SOURCE_ITER_DIR=$(cd -- "$(dirname -- "${SOURCE_SUMMARY}")" >/dev/null 2>&1 && pwd)
  shopt -s nullglob
  SOURCE_DUP_INPUTS=("${SOURCE_ITER_DIR}"/input_*.txnctx)
  shopt -u nullglob
  [[ "${#SOURCE_DUP_INPUTS[@]}" == "6" ]] \
    || die "${MODE_VALUE} requires 6 source input_*.txnctx files in ${SOURCE_ITER_DIR}, found ${#SOURCE_DUP_INPUTS[@]}"

  ensure_bridge_bin

  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  TARGET_PACKET_BATCH_FILE="${LOG_DIR}/packets_split.target.txt"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"
  : >"${TARGET_PACKET_BATCH_FILE}"

  first_adapt=""
  TARGET_DUPLICATE_TOTAL_LAMPORTS=0
  for idx in "${!SOURCE_DUP_INPUTS[@]}"; do
    packet_path="${LOG_DIR}/packet_${idx}.target.txt"
    bridge_out="${LOG_DIR}/bridge_${idx}.target.out"
    "${BRIDGE_BIN}" \
      --input "${SOURCE_DUP_INPUTS[$idx]}" \
      --output "${packet_path}" \
      --adapt-mode local-system-transfer \
      --recent-blockhash "${TARGET_BLOCKHASH}" \
      --from-genesis-account 0 \
      --to-genesis-account 1 \
      >"${bridge_out}"
    cat "${packet_path}" >>"${TARGET_PACKET_BATCH_FILE}"
    local_adapt=$(grep '^adapted family=' "${bridge_out}" || true)
    [[ -n "${local_adapt}" ]] || die "failed to capture duplicate-seq adapted system summary for packet ${idx}"
    local_lamports=$(extract_bridge_field lamports "${local_adapt}")
    [[ "${local_lamports}" =~ ^[0-9]+$ ]] || die "failed to parse duplicate-seq lamports for packet ${idx}"
    TARGET_DUPLICATE_TOTAL_LAMPORTS=$((TARGET_DUPLICATE_TOTAL_LAMPORTS + local_lamports))
    if [[ -z "${first_adapt}" ]]; then
      first_adapt="${local_adapt}"
    fi
  done

  [[ -n "${first_adapt}" ]] || die "failed to capture first duplicate-seq adapted system summary"
  PAYER=$(extract_bridge_field from "${first_adapt}")
  RECIPIENT_ONE=$(extract_bridge_field to "${first_adapt}")
  RECIPIENT_TWO=""
  EXTRA_PREFUND_KEYS+=("${RECIPIENT_ONE}")
  EXTRA_PREFUND_AMOUNTS+=("1000000000")

  if [[ -z "${SEQ_ONE}" ]]; then
    SEQ_ONE=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*seq_id=\([0-9][0-9]*\).*/\1/p')
  fi
  [[ "${SEQ_ONE}" =~ ^[0-9]+$ ]] || die "failed to derive ${MODE_VALUE} seq_id"

  render_target_duplicate_seq_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${SEQ_ONE}" \
    "${TARGET_PACKET_BATCH_FILE}" \
    "${MODE_VALUE}"
elif [[ "${MODE_VALUE}" == "durable_nonce_commit" || "${MODE_VALUE}" == "durable_nonce_reconnect" || "${MODE_VALUE}" == "durable_nonce_replay_after_reconnect" || "${MODE_VALUE}" == "durable_nonce_wrong_authority" || "${MODE_VALUE}" == "durable_nonce_wrong_authority_reconnect" ]]; then
  ensure_bridge_bin

  TARGET_PACKET_FILE="${LOG_DIR}/packet_one.target.txt"
  TARGET_BRIDGE_OUT="${LOG_DIR}/durable-nonce.target.gen.out"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"
  DURABLE_LAMPORTS=""
  if [[ "${SRC_RECIPIENT_ONE_INITIAL}" =~ ^[0-9]+$ && "${SRC_RECIPIENT_ONE_EXPECTED}" =~ ^[0-9]+$ ]]; then
    DURABLE_LAMPORTS=$((SRC_RECIPIENT_ONE_EXPECTED - SRC_RECIPIENT_ONE_INITIAL))
  fi
  if [[ -z "${DURABLE_LAMPORTS}" || ! "${DURABLE_LAMPORTS}" =~ ^[0-9]+$ || "${DURABLE_LAMPORTS}" == "0" ]]; then
    DURABLE_LAMPORTS=$(printf '%s\n' "$(summary_get input_note)" | sed -n 's/.* lamports=\([0-9][0-9]*\).*/\1/p')
  fi
  [[ "${DURABLE_LAMPORTS}" =~ ^[0-9]+$ && "${DURABLE_LAMPORTS}" -gt 0 ]] \
    || die "failed to derive durable nonce lamports from source summary"

  WRONG_NONCE_AUTHORITY=0
  if [[ "${MODE_VALUE}" == "durable_nonce_wrong_authority" || "${MODE_VALUE}" == "durable_nonce_wrong_authority_reconnect" ]]; then
    WRONG_NONCE_AUTHORITY=1
  fi

  prepare_target_durable_nonce_commit \
    "${TARGET_PACKET_FILE}" \
    "${TARGET_BRIDGE_OUT}" \
    "${DURABLE_LAMPORTS}" \
    "${SEED_VALUE:-1}" \
    "${WRONG_NONCE_AUTHORITY}"

  render_target_system_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${MODE_VALUE}" \
    "${SEQ_ONE:-1}" \
    "${SEQ_TWO:-2}" \
    "${TARGET_PACKET_FILE}" \
    "${TARGET_PACKET_FILE}"
elif [[ "${MODE_VALUE}" == "bam_fee_config_refresh_queue_burst" || "${MODE_VALUE}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${MODE_VALUE}" == "bam_fee_config_midqueue_refresh" || "${MODE_VALUE}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${MODE_VALUE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
  SOURCE_ITER_DIR=$(cd -- "$(dirname -- "${SOURCE_SUMMARY}")" >/dev/null 2>&1 && pwd)
  SOURCE_INPUT_ONE="${SOURCE_ITER_DIR}/input.txnctx"
  [[ -f "${SOURCE_INPUT_ONE}" ]] || die "missing fee-refresh queue source input ${SOURCE_INPUT_ONE}"

  BATCH_COUNT=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*batch_count=\([0-9][0-9]*\).*/\1/p')
  [[ "${BATCH_COUNT}" =~ ^[0-9]+$ && "${BATCH_COUNT}" -ge 1 ]] \
    || die "failed to derive fee-refresh queue batch count"
  CLOSE_AFTER_RESULTS=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*close_after_results=\([0-9][0-9]*\).*/\1/p')
  if [[ -z "${CLOSE_AFTER_RESULTS}" ]]; then
    CLOSE_AFTER_RESULTS=1
  fi
  [[ "${CLOSE_AFTER_RESULTS}" =~ ^[0-9]+$ ]] || die "failed to derive fee-refresh queue close_after_results"
  (( CLOSE_AFTER_RESULTS <= BATCH_COUNT )) || die "fee-refresh queue close_after_results exceeds batch count"
  SECOND_CLOSE_AFTER_RESULTS=0
  if is_queue_multi_reconnect_mode "${MODE_VALUE}"; then
    SECOND_CLOSE_AFTER_RESULTS=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*second_close_after_results=\([0-9][0-9]*\).*/\1/p')
    if [[ -z "${SECOND_CLOSE_AFTER_RESULTS}" ]]; then
      SECOND_CLOSE_AFTER_RESULTS=1
    fi
    [[ "${SECOND_CLOSE_AFTER_RESULTS}" =~ ^[0-9]+$ ]] || die "failed to derive ${MODE_VALUE} second_close_after_results"
    (( SECOND_CLOSE_AFTER_RESULTS <= BATCH_COUNT )) || die "${MODE_VALUE} second_close_after_results exceeds batch count"
  fi
  MAX_SCHEDULE_SLOT_VALUE=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*max_schedule_slot=\([^ ]*\).*/\1/p')
  if [[ -z "${MAX_SCHEDULE_SLOT_VALUE}" ]]; then
    MAX_SCHEDULE_SLOT_VALUE=max
  fi

  ensure_bridge_bin
  SEQ_ONE="${SEQ_ONE:-200}"
  SEQ_TWO="${SEQ_TWO:-201}"
  PARTIAL_DRAIN_LAST_SEQ=$((SEQ_TWO + BATCH_COUNT - 1))
  TARGET_OLD_CONFIG_PACKET_FILE="${LOG_DIR}/packet_old_config.target.txt"
  TARGET_OLD_CONFIG_BRIDGE_OUT="${LOG_DIR}/bridge_old_config.target.out"
  TARGET_PACKET_TWO_FILE="${TARGET_OLD_CONFIG_PACKET_FILE}"
  TARGET_NORMAL_TPU_PACKET_FILE="${LOG_DIR}/normal_tpu.target.txt"
  TARGET_NORMAL_TPU_BRIDGE_OUT="${LOG_DIR}/normal_tpu.target.bridge.out"
  TARGET_NORMAL_TPU_TO_INDEX=$((1 + BATCH_COUNT + 1))
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  TARGET_QUEUE_PACKET_FILES=()

  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_ONE}" \
    --output "${TARGET_OLD_CONFIG_PACKET_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 2 \
    >"${TARGET_OLD_CONFIG_BRIDGE_OUT}"
  old_config_adapt=$(grep '^adapted family=' "${TARGET_OLD_CONFIG_BRIDGE_OUT}" || true)
  [[ -n "${old_config_adapt}" ]] || die "failed to capture fee-refresh old-config adapted summary"
  PAYER=$(extract_bridge_field from "${old_config_adapt}")
  RECIPIENT_TWO=$(extract_bridge_field to "${old_config_adapt}")
  EXTRA_PREFUND_KEYS+=("${RECIPIENT_TWO}")
  EXTRA_PREFUND_AMOUNTS+=("1000000000")

  for batch_idx in $(seq 0 $((BATCH_COUNT - 1))); do
    seq_id=$((SEQ_TWO + batch_idx))
    packet_path="${LOG_DIR}/packet_${seq_id}.target.txt"
    bridge_out="${LOG_DIR}/bridge_${seq_id}.target.out"
    to_index=$((1 + batch_idx))
    if (( to_index >= 2 )); then
      to_index=$((to_index + 1))
    fi

    "${BRIDGE_BIN}" \
      --input "${SOURCE_INPUT_ONE}" \
      --output "${packet_path}" \
      --adapt-mode local-system-transfer \
      --recent-blockhash "${TARGET_BLOCKHASH}" \
      --from-genesis-account 0 \
      --to-genesis-account "${to_index}" \
      >"${bridge_out}"
    adapt_line=$(grep '^adapted family=' "${bridge_out}" || true)
    [[ -n "${adapt_line}" ]] || die "failed to capture fee-refresh queue adapted summary for seq_id=${seq_id}"
    recipient=$(extract_bridge_field to "${adapt_line}")
    if (( batch_idx == 0 )); then
      RECIPIENT_ONE="${recipient}"
      TARGET_PACKET_FILE="${packet_path}"
    fi
    EXTRA_PREFUND_KEYS+=("${recipient}")
    EXTRA_PREFUND_AMOUNTS+=("1000000000")
    TARGET_QUEUE_PACKET_FILES+=("${packet_path}")
  done
  if is_queue_source_mix_mode "${MODE_VALUE}"; then
    "${BRIDGE_BIN}" \
      --input "${SOURCE_INPUT_ONE}" \
      --output "${TARGET_NORMAL_TPU_PACKET_FILE}" \
      --adapt-mode local-system-transfer \
      --recent-blockhash "${TARGET_BLOCKHASH}" \
      --from-genesis-account 0 \
      --to-genesis-account "${TARGET_NORMAL_TPU_TO_INDEX}" \
      >"${TARGET_NORMAL_TPU_BRIDGE_OUT}"
    normal_tpu_adapt=$(grep '^adapted family=' "${TARGET_NORMAL_TPU_BRIDGE_OUT}" || true)
    [[ -n "${normal_tpu_adapt}" ]] || die "failed to capture normal TPU adapted summary for ${MODE_VALUE}"
    normal_tpu_recipient=$(extract_bridge_field to "${normal_tpu_adapt}")
    EXTRA_PREFUND_KEYS+=("${normal_tpu_recipient}")
    EXTRA_PREFUND_AMOUNTS+=("1000000000")
  fi

  [[ -n "${PAYER}" && -n "${RECIPIENT_ONE}" && -n "${RECIPIENT_TWO}" ]] \
    || die "failed to derive fee-refresh queue payer/recipients"
  prefund_if_needed "${PAYER}" "${PAYER_PREFUND_LAMPORTS}"
  for idx in "${!EXTRA_PREFUND_KEYS[@]}"; do
    prefund_if_needed "${EXTRA_PREFUND_KEYS[$idx]}" "${EXTRA_PREFUND_AMOUNTS[$idx]}"
  done
  EXTRA_PREFUND_KEYS=()
  EXTRA_PREFUND_AMOUNTS=()

  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  TARGET_QUEUE_PACKET_FILES=()
  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_ONE}" \
    --output "${TARGET_OLD_CONFIG_PACKET_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 2 \
    >"${TARGET_OLD_CONFIG_BRIDGE_OUT}"
  for batch_idx in $(seq 0 $((BATCH_COUNT - 1))); do
    seq_id=$((SEQ_TWO + batch_idx))
    packet_path="${LOG_DIR}/packet_${seq_id}.target.txt"
    bridge_out="${LOG_DIR}/bridge_${seq_id}.target.out"
    to_index=$((1 + batch_idx))
    if (( to_index >= 2 )); then
      to_index=$((to_index + 1))
    fi
    "${BRIDGE_BIN}" \
      --input "${SOURCE_INPUT_ONE}" \
      --output "${packet_path}" \
      --adapt-mode local-system-transfer \
      --recent-blockhash "${TARGET_BLOCKHASH}" \
      --from-genesis-account 0 \
      --to-genesis-account "${to_index}" \
      >"${bridge_out}"
    TARGET_QUEUE_PACKET_FILES+=("${packet_path}")
  done
  if is_queue_source_mix_mode "${MODE_VALUE}"; then
    "${BRIDGE_BIN}" \
      --input "${SOURCE_INPUT_ONE}" \
      --output "${TARGET_NORMAL_TPU_PACKET_FILE}" \
      --adapt-mode local-system-transfer \
      --recent-blockhash "${TARGET_BLOCKHASH}" \
      --from-genesis-account 0 \
      --to-genesis-account "${TARGET_NORMAL_TPU_TO_INDEX}" \
      >"${TARGET_NORMAL_TPU_BRIDGE_OUT}"
  fi

  render_target_fee_config_refresh_queue_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${SEQ_ONE}" \
    "${SEQ_TWO}" \
    "${TARGET_OLD_CONFIG_PACKET_FILE}" \
    "${BATCH_COUNT}" \
    "${CLOSE_AFTER_RESULTS}" \
    "${MAX_SCHEDULE_SLOT_VALUE}"
elif is_fee_config_two_recipient_mode "${MODE_VALUE}"; then
  ensure_bridge_bin

  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  TARGET_PACKET_FILE="${LOG_DIR}/packet_one.target.txt"
  TARGET_PACKET_TWO_FILE="${LOG_DIR}/packet_two.target.txt"
  TARGET_TXNCTX_ONE="${LOG_DIR}/fee-url-churn-tx0.target.txnctx"
  TARGET_TXNCTX_TWO="${LOG_DIR}/fee-url-churn-tx1.target.txnctx"
  TARGET_GEN_ONE_OUT="${LOG_DIR}/fee-url-churn-tx0.target.gen.out"
  TARGET_GEN_TWO_OUT="${LOG_DIR}/fee-url-churn-tx1.target.gen.out"
  TARGET_BRIDGE_OUT="${LOG_DIR}/bridge_one.target.out"
  TARGET_BRIDGE_TWO_OUT="${LOG_DIR}/bridge_two.target.out"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  CHURN_LAMPORTS=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*lamports=\([0-9][0-9]*\).*/\1/p')
  [[ "${CHURN_LAMPORTS}" =~ ^[0-9]+$ ]] || CHURN_LAMPORTS=1000000

  generate_raw_transfer_packet \
    "${TARGET_TXNCTX_ONE}" \
    "${TARGET_PACKET_FILE}" \
    "${TARGET_GEN_ONE_OUT}" \
    "${TARGET_BRIDGE_OUT}" \
    "${TARGET_BLOCKHASH}" \
    1 \
    "${CHURN_LAMPORTS}" \
    0 \
    "" \
    300000 \
    50000
  generate_raw_transfer_packet \
    "${TARGET_TXNCTX_TWO}" \
    "${TARGET_PACKET_TWO_FILE}" \
    "${TARGET_GEN_TWO_OUT}" \
    "${TARGET_BRIDGE_TWO_OUT}" \
    "${TARGET_BLOCKHASH}" \
    2 \
    "${CHURN_LAMPORTS}" \
    0 \
    "" \
    300000 \
    50000

  TARGET_GEN_LINE_ONE=$(grep '^wrote synthetic txnctx ' "${TARGET_GEN_ONE_OUT}" || true)
  TARGET_GEN_LINE_TWO=$(grep '^wrote synthetic txnctx ' "${TARGET_GEN_TWO_OUT}" || true)
  [[ -n "${TARGET_GEN_LINE_ONE}" && -n "${TARGET_GEN_LINE_TWO}" ]] \
    || die "failed to capture target fee-url-churn generator summaries"
  PAYER=$(extract_gen_field from "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_ONE=$(extract_gen_field target "${TARGET_GEN_LINE_ONE}")
  RECIPIENT_TWO=$(extract_gen_field target "${TARGET_GEN_LINE_TWO}")

  render_target_system_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${MODE_VALUE}" \
    "${SEQ_ONE:-1}" \
    "${SEQ_TWO:-2}" \
    "${TARGET_PACKET_FILE}" \
    "${TARGET_PACKET_TWO_FILE}"
elif [[ -n "${SOURCE_INPUT_PATH}" \
  && -f "${SOURCE_INPUT_PATH}" \
  && -n "${SOURCE_SYSTEM_KIND}" \
  ]] && is_local_system_kind "${SOURCE_SYSTEM_KIND}"; then
  ensure_bridge_bin

  if [[ "${MODE_VALUE}" == "stale_slot_reject" || "${MODE_VALUE}" == "mixed_stale_multi_batch" || "${MODE_VALUE}" == "mixed_stale_reconnect" || "${MODE_VALUE}" == "mixed_terminal_producers_reconnect" || ( "${MODE_VALUE}" == "random_mixed_multi_batch" && $(random_mixed_token_count "${SOURCE_RANDOM_MIXED_PATTERN}" stale) -gt 0 ) ]]; then
    WAITED_SLOT=$(wait_rpc_slot_at_least 1 "${TIMEOUT_SECS}") || die "jito-agave slot did not advance past genesis for stale-slot test; latest slot=${WAITED_SLOT}"
  fi
  TARGET_BLOCKHASH=$(rpc_latest_blockhash)
  TARGET_PACKET_FILE="${LOG_DIR}/packet_one.target.txt"
  TARGET_PACKET_TWO_FILE="${LOG_DIR}/packet_two.target.txt"
  TARGET_BRIDGE_OUT="${LOG_DIR}/bridge_one.target.out"
  TARGET_BRIDGE_TWO_OUT="${LOG_DIR}/bridge_two.target.out"
  EFFECTIVE_SCENARIO_FILE="${LOG_DIR}/scenario.target.toml"

  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_PATH}" \
    --output "${TARGET_PACKET_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 1 \
    >"${TARGET_BRIDGE_OUT}"

  "${BRIDGE_BIN}" \
    --input "${SOURCE_INPUT_PATH}" \
    --output "${TARGET_PACKET_TWO_FILE}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${TARGET_BLOCKHASH}" \
    --from-genesis-account 0 \
    --to-genesis-account 2 \
    >"${TARGET_BRIDGE_TWO_OUT}"

  TARGET_ADAPT=$(grep '^adapted family=' "${TARGET_BRIDGE_OUT}" || true)
  [[ -n "${TARGET_ADAPT}" ]] || die "failed to capture target adapted system summary"
  TARGET_ADAPT_TWO=$(grep '^adapted family=' "${TARGET_BRIDGE_TWO_OUT}" || true)
  [[ -n "${TARGET_ADAPT_TWO}" ]] || die "failed to capture second target adapted system summary"
  PAYER=$(extract_bridge_field from "${TARGET_ADAPT}")
  RECIPIENT_ONE=$(extract_bridge_field to "${TARGET_ADAPT}")
  RECIPIENT_TWO=$(extract_bridge_field to "${TARGET_ADAPT_TWO}")
  if has_account_metadata_kind "${SOURCE_SYSTEM_KIND}"; then
    RECIPIENT_ONE_OWNER_EXPECTED=$(extract_bridge_field owner "${TARGET_ADAPT}")
    RECIPIENT_ONE_SPACE_EXPECTED=$(extract_bridge_field space "${TARGET_ADAPT}")
    RECIPIENT_TWO_OWNER_EXPECTED=$(extract_bridge_field owner "${TARGET_ADAPT_TWO}")
    RECIPIENT_TWO_SPACE_EXPECTED=$(extract_bridge_field space "${TARGET_ADAPT_TWO}")
  else
    RECIPIENT_ONE_OWNER_EXPECTED=""
    RECIPIENT_ONE_SPACE_EXPECTED=""
    RECIPIENT_TWO_OWNER_EXPECTED=""
    RECIPIENT_TWO_SPACE_EXPECTED=""
  fi

  render_target_system_scenario \
    "${EFFECTIVE_SCENARIO_FILE}" \
    "${MODE_VALUE:-unknown}" \
    "${SEQ_ONE:-1}" \
    "${SEQ_TWO:-2}" \
    "${TARGET_PACKET_FILE}" \
    "${TARGET_PACKET_TWO_FILE}"
fi

case "${MODE_VALUE}" in
  unique_after_reconnect|seq_id_wrap_sequence|seq_id_out_of_order_multi_batch|seq_id_wrap_out_of_order_multi_batch|disable_enable_unique_after_reconnect|url_churn_unique_after_reconnect|mixed_multi_batch|mixed_empty_multi_batch|mixed_malformed_multi_batch|mixed_bad_signature_multi_batch|mixed_bad_signature_reconnect|mixed_stale_multi_batch|mixed_stale_reconnect|mixed_terminal_producers_reconnect|random_mixed_multi_batch|non_atomic_valid_multi_packet)
    ;;
  *)
    RECIPIENT_TWO_OWNER_EXPECTED=""
    RECIPIENT_TWO_SPACE_EXPECTED=""
    ;;
esac
if [[ "${MODE_VALUE}" == "stale_slot_reject" || "${MODE_VALUE}" == "stale_slot_reject_reconnect" || "${MODE_VALUE}" == "empty_batch_reject" || "${MODE_VALUE}" == "empty_batch_reject_reconnect" || "${MODE_VALUE}" == "malformed_first_atomic" || "${MODE_VALUE}" == "malformed_first_atomic_reconnect" || "${MODE_VALUE}" == "malformed_tail_atomic" || "${MODE_VALUE}" == "malformed_tail_atomic_reconnect" || "${MODE_VALUE}" == "bad_signature_first_atomic" || "${MODE_VALUE}" == "bad_signature_first_atomic_reconnect" || "${MODE_VALUE}" == "bad_signature_tail_atomic" || "${MODE_VALUE}" == "bad_signature_tail_atomic_reconnect" ]]; then
  RECIPIENT_ONE_OWNER_EXPECTED=""
  RECIPIENT_ONE_SPACE_EXPECTED=""
  RECIPIENT_TWO_OWNER_EXPECTED=""
  RECIPIENT_TWO_SPACE_EXPECTED=""
	fi

	prefund_if_needed "${PAYER}" "${PAYER_PREFUND_LAMPORTS}"
	for idx in "${!EXTRA_PREFUND_KEYS[@]}"; do
	  prefund_if_needed "${EXTRA_PREFUND_KEYS[$idx]}" "${EXTRA_PREFUND_AMOUNTS[$idx]}"
	done
prefund_if_needed "${RECIPIENT_ONE}" "${SRC_RECIPIENT_ONE_INITIAL}"
prefund_if_needed "${RECIPIENT_TWO}" "${SRC_RECIPIENT_TWO_INITIAL}"

PAYER_INITIAL=""
PAYER_OBSERVED=""
if [[ -n "${PAYER}" && "${PAYER}" != "n/a" ]]; then
  PAYER_INITIAL=$(rpc_balance "${PAYER}" 2>/dev/null || printf '0')
fi

recipient_one_initial=""
recipient_two_initial=""
recipient_one_expected=""
recipient_two_expected=""
if [[ -n "${RECIPIENT_ONE}" && "${RECIPIENT_ONE}" != "n/a" ]]; then
  recipient_one_initial=$(rpc_balance "${RECIPIENT_ONE}" 2>/dev/null || printf '0')
  if [[ ( "${MODE_VALUE}" == "duplicate_seq_split" || "${MODE_VALUE}" == "duplicate_seq_split_reconnect" ) && "${TARGET_DUPLICATE_TOTAL_LAMPORTS:-}" =~ ^[0-9]+$ ]]; then
    recipient_one_expected=$((recipient_one_initial + TARGET_DUPLICATE_TOTAL_LAMPORTS))
  elif [[ "${SRC_RECIPIENT_ONE_INITIAL}" =~ ^[0-9]+$ && "${SRC_RECIPIENT_ONE_EXPECTED}" =~ ^[0-9]+$ ]]; then
    recipient_one_expected=$((recipient_one_initial + SRC_RECIPIENT_ONE_EXPECTED - SRC_RECIPIENT_ONE_INITIAL))
  fi
fi
			if [[ -n "${RECIPIENT_TWO}" && "${RECIPIENT_TWO}" != "n/a" ]]; then
			  recipient_two_initial=$(rpc_balance "${RECIPIENT_TWO}" 2>/dev/null || printf '0')
			  if [[ "${SRC_RECIPIENT_TWO_INITIAL}" =~ ^[0-9]+$ && "${SRC_RECIPIENT_TWO_EXPECTED}" =~ ^[0-9]+$ ]]; then
			    recipient_two_expected=$((recipient_two_initial + SRC_RECIPIENT_TWO_EXPECTED - SRC_RECIPIENT_TWO_INITIAL))
			  fi
			fi
	BAM_FEE_RECIPIENT=""
	BAM_FEE_RECIPIENT_INITIAL=""
	BAM_FEE_RECIPIENT_OBSERVED=""
	BAM_FEE_RECIPIENT_SECOND=""
	BAM_FEE_RECIPIENT_SECOND_INITIAL=""
	BAM_FEE_RECIPIENT_SECOND_OBSERVED=""
	if [[ "${MODE_VALUE}" == "bam_fee_priority_replay_after_reconnect" && -n "${RECIPIENT_ONE}" && "${RECIPIENT_ONE}" != "n/a" ]]; then
	  BAM_FEE_RECIPIENT="${RECIPIENT_ONE}"
	  BAM_FEE_RECIPIENT_INITIAL="${recipient_one_initial}"
	elif { [[ "${MODE_VALUE}" == "bam_fee_priority_commit" ]] || is_fee_queue_burst_mode "${MODE_VALUE}"; } && [[ -n "${RECIPIENT_TWO}" && "${RECIPIENT_TWO}" != "n/a" ]]; then
	  BAM_FEE_RECIPIENT="${RECIPIENT_TWO}"
	  BAM_FEE_RECIPIENT_INITIAL="${recipient_two_initial}"
	  recipient_two_expected=""
	fi
	if is_fee_config_commission_only_mode "${MODE_VALUE}" && [[ -n "${RECIPIENT_TWO}" && "${RECIPIENT_TWO}" != "n/a" ]]; then
	  BAM_FEE_RECIPIENT="${RECIPIENT_TWO}"
	  BAM_FEE_RECIPIENT_INITIAL="${recipient_two_initial}"
	  recipient_two_expected=""
	fi
	if is_fee_config_two_recipient_mode "${MODE_VALUE}" && [[ -n "${RECIPIENT_ONE}" && "${RECIPIENT_ONE}" != "n/a" && -n "${RECIPIENT_TWO}" && "${RECIPIENT_TWO}" != "n/a" ]]; then
	  BAM_FEE_RECIPIENT="${RECIPIENT_TWO}"
	  BAM_FEE_RECIPIENT_INITIAL="${recipient_two_initial}"
	  BAM_FEE_RECIPIENT_SECOND="${RECIPIENT_ONE}"
	  BAM_FEE_RECIPIENT_SECOND_INITIAL="${recipient_one_initial}"
	  recipient_one_expected=""
	  recipient_two_expected=""
	fi
	if [[ "${MODE_VALUE}" == "seq_id_wrap_conflicting_spend_multi_batch" ]]; then
  TARGET_CONFLICT_WIN_LAMPORTS=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*tx0_lamports=\([0-9][0-9]*\).*/\1/p')
  if [[ "${TARGET_CONFLICT_WIN_LAMPORTS}" =~ ^[0-9]+$ ]]; then
    if [[ -n "${RECIPIENT_ONE}" && "${RECIPIENT_ONE}" != "n/a" ]]; then
      recipient_one_expected=$((recipient_one_initial + TARGET_CONFLICT_WIN_LAMPORTS))
    fi
    if [[ -n "${RECIPIENT_TWO}" && "${RECIPIENT_TWO}" != "n/a" ]]; then
      recipient_two_expected="${recipient_two_initial}"
    fi
  fi
fi

			START_SLOT=$(rpc_slot 2>/dev/null || printf '0')
	if is_fee_url_churn_mode "${MODE_VALUE}"; then
	  SECONDARY_SCENARIO_FILE="${LOG_DIR}/scenario-secondary.target.toml"
	  render_target_bam_fee_url_churn_second_scenario \
	    "${SECONDARY_SCENARIO_FILE}" \
	    "${SEQ_TWO:-2}" \
	    "${TARGET_PACKET_TWO_FILE}"
	  bam_secondary_cmd=(
	    cargo run --quiet --manifest-path "${BAM_MANIFEST}" --bin bam-test-server --
	    --scenario benign
	    --scenario-file "${SECONDARY_SCENARIO_FILE}"
	    --bind "$(url_bind_addr "${BAM_BAD_URL}")"
	    --tpu-ip 127.0.0.1
	    --tpu-port "${TPU_PORT}"
	    --tpu-fwd-ip 127.0.0.1
	    --tpu-fwd-port "${TPU_FWD_PORT}"
	    --shred-ip 127.0.0.1
	    --shred-port "${SHRED_PORT}"
	    --builder-commission-pct "${BAM_BUILDER_COMMISSION_PCT}"
	    --commission-bps "${BAM_COMMISSION_BPS}"
	    --builder-pubkey "${BAM_FEE_RECIPIENT_SECOND}"
	  )
	  "${bam_secondary_cmd[@]}" >"${BAM_SECONDARY_LOG}" 2>&1 &
	  BAM_SECONDARY_PID=$!
	  wait_for_pattern "${BAM_SECONDARY_LOG}" '^BAM test server listening on ' "${TIMEOUT_SECS}" \
	    || die "secondary Jito BAM fee-config server did not start"
	fi

	bam_cmd=(
	  cargo run --quiet --manifest-path "${BAM_MANIFEST}" --bin bam-test-server --
	  --scenario benign
	  --scenario-file "${EFFECTIVE_SCENARIO_FILE}"
	  --bind "${BAM_BIND}"
	  --tpu-ip 127.0.0.1
	  --tpu-port "${TPU_PORT}"
	  --tpu-fwd-ip 127.0.0.1
	  --tpu-fwd-port "${TPU_FWD_PORT}"
	  --shred-ip 127.0.0.1
	  --shred-port "${SHRED_PORT}"
	  --builder-commission-pct "${BAM_BUILDER_COMMISSION_PCT}"
	  --commission-bps "${BAM_COMMISSION_BPS}"
	)
	if [[ -n "${BAM_FEE_RECIPIENT}" ]]; then
	  bam_cmd+=(--builder-pubkey "${BAM_FEE_RECIPIENT}")
	fi
	if is_fee_config_refresh_mode "${MODE_VALUE}"; then
	  bam_cmd+=(--omit-block-engine-config)
	fi
	"${bam_cmd[@]}" >"${BAM_LOG}" 2>&1 &
	BAM_PID=$!

wait_for_pattern "${BAM_LOG}" '^BAM test server listening on ' "${TIMEOUT_SECS}" \
  || die "BAM test server did not start"

if [[ "${TEST_VALIDATOR_HAS_BAM_URL}" == "0" ]]; then
  {
    printf 'operator: solana-test-validator lacks --bam-url; configuring BAM through admin RPC\n'
    printf 'operator: set_bam --url %s\n' "${BAM_URL}"
  } >>"${OPERATOR_LOG}"
  bam_config_deadline=$((SECONDS + TIMEOUT_SECS))
  bam_configured=0
  while (( SECONDS < bam_config_deadline )); do
    if "${AGAVE_VALIDATOR_BIN}" --ledger "${LEDGER_DIR}" \
        set-bam-config --bam-url "${BAM_URL}" >>"${OPERATOR_LOG}" 2>&1; then
      bam_configured=1
      break
    fi
    sleep 1
  done
  [[ "${bam_configured}" == "1" ]] \
    || die "failed to configure Jito BAM through the validator admin RPC"
fi

wait_for_pattern "${BAM_LOG}" '^InitSchedulerStream:' "${TIMEOUT_SECS}" \
  || die "jito-agave did not open a BAM scheduler stream"
case "${MODE_VALUE}" in
  disable_enable_unique_after_reconnect)
    start_jito_disable_enable_operator
    ;;
  disable_enable_queue_burst_reconnect)
    start_jito_disable_enable_queue_operator
    ;;
  disable_enable_tpu_release)
    start_jito_disable_enable_tpu_release_operator
    ;;
  url_churn_unique_after_reconnect)
    start_jito_url_churn_operator
    ;;
  bam_fee_url_churn_priority_commit|bam_fee_url_churn_same_slot_priority_commit)
    start_jito_fee_url_churn_operator
    ;;
esac
if [[ "${MODE_VALUE}" == "source_mix_precommit" ]]; then
  send_normal_tpu_packet_after_scheduler_ready \
    "${TARGET_PACKET_TWO_FILE}" \
    "${BAM_LOG}" \
    "${FD_LOG}" \
    "${LOG_DIR}/normal_tpu.log"
elif [[ "${MODE_VALUE}" == "source_mix_atomic_revert_precommit" ]]; then
  send_normal_tpu_packet_after_scheduler_ready \
    "${TARGET_PACKET_FILE}" \
    "${BAM_LOG}" \
    "${FD_LOG}" \
    "${LOG_DIR}/normal_tpu.log"
elif [[ "${MODE_VALUE}" == "disable_enable_tpu_release" ]]; then
  send_normal_tpu_packet_after_bam_disable \
    "${TARGET_PACKET_TWO_FILE}" \
    "${BAM_LOG}" \
    "${FD_LOG}" \
    "${OPERATOR_LOG}" \
    "${LOG_DIR}/normal_tpu.log"
fi
if is_queue_source_mix_mode "${MODE_VALUE}"; then
  send_normal_tpu_packet_after_queue_close \
    "${TARGET_NORMAL_TPU_PACKET_FILE}" \
    "${BAM_LOG}" \
    "${FD_LOG}" \
    "${LOG_DIR}/normal_tpu.log"
fi
if [[ "${MODE_VALUE}" == "external_scenario" ]] && ! scenario_has_send_events "${EFFECTIVE_SCENARIO_FILE}"; then
  echo "external scenario has no live BAM send events; preserving handshake-only outcome"
elif [[ "${MODE_VALUE}" == "atomic_blockhash_mid_fail" || "${MODE_VALUE}" == "atomic_resolver_mid_fail" || "${MODE_VALUE}" == "atomic_duplicate_sig_mid_fail" || "${MODE_VALUE}" == "queue_reconnect_timing_jitter" ]]; then
	  if ! wait_for_pattern "${BAM_LOG}" 'scheduler<-validator batch_result' 12; then
	    echo "warning: jito-agave did not return an early ${MODE_VALUE} batch result; preserving incomplete outcome for diff"
	  fi
else
  wait_for_pattern "${BAM_LOG}" 'scheduler<-validator batch_result' "${TIMEOUT_SECS}" \
    || die "jito-agave did not return a BAM batch result"
fi
case "${MODE_VALUE}" in
	  replay_same_conn|replay_after_reconnect|seq_id_max_replay_after_reconnect)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=not_committed .*ALREADY_PROCESSED" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected replay rejection for seq_id=${SEQ_TWO}"
    ;;
	  non_atomic_valid_multi_packet)
	    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed" "${TIMEOUT_SECS}" \
	      || die "jito-agave did not commit the valid non-atomic multi-packet batch"
	    ;;
  non_atomic_partial_overdraft_reconnect)
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for ${MODE_VALUE}"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "${MODE_VALUE} target did not resume scripted events on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return a terminal ${MODE_VALUE} batch result"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal ${MODE_VALUE} batch results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  non_atomic_first_overdraft|non_atomic_mid_overdraft|non_atomic_partial_overdraft|non_atomic_partial_resolver_fail|non_atomic_partial_blockhash_fail|non_atomic_partial_duplicate_sig|non_atomic_partial_cu_fail)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return a terminal ${MODE_VALUE} batch result"
    wait_for_pattern "${BAM_LOG}" 'scripted wait_inbound satisfied conn=1 kind=batch_result observed=1' "${TIMEOUT_SECS}" \
      || die "jito-agave did not observe the terminal ${MODE_VALUE} batch result"
    ;;
  non_atomic_single_packet)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the single-packet non-atomic batch"
    ;;
  bam_cu_limit_fail)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=transaction_error index=0" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected low-CU transaction_error"
    ;;
  bam_cu_limit_fail_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave low-CU reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for low-CU result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave low-CU reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=transaction_error index=0" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected low-CU reconnect transaction_error"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal low-CU reconnect results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  fee_only_commit)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the fee-only batch"
    wait_for_pattern "${BAM_LOG}" "scripted wait_inbound satisfied conn=1 kind=committed_batch observed=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not observe the fee-only committed batch"
    ;;
  fee_only_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave fee-only reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for fee-only result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave fee-only reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the fee-only reconnect batch"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal fee-only reconnect results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  bam_fee_priority_replay_after_reconnect)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1 conn=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first priority-fee replay batch"
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave priority-fee replay scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect before priority-fee replay"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave priority-fee replay scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=not_committed .*ALREADY_PROCESSED" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the priority-fee replay"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=(${SEQ_ONE}|${SEQ_TWO}) status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "2" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal priority-fee replay results; expected exactly two"
    ;;
  bam_fee_url_churn_priority_commit|bam_fee_url_churn_same_slot_priority_commit)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}"       || die "jito-agave did not commit the first priority-fee batch before fee URL churn"
    wait_for_pattern "${OPERATOR_LOG}" "^operator: set_bam --url ${BAM_BAD_URL_RE}" "${TIMEOUT_SECS}"       || die "jito operator did not issue the alternate fee-config BAM URL"
    wait_for_pattern "${BAM_SECONDARY_LOG}" '^InitSchedulerStream:' "${TIMEOUT_SECS}"       || die "jito-agave did not connect to the alternate fee-config BAM server"
    wait_for_pattern "${BAM_SECONDARY_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed txns=1" "${TIMEOUT_SECS}"       || die "jito-agave did not commit the second priority-fee batch after fee URL churn"
    if [[ "${MODE_VALUE}" == "bam_fee_url_churn_same_slot_priority_commit" ]]; then
      primary_slot=$(batch_result_slot_from_log "${BAM_LOG}" "${SEQ_ONE}")
      secondary_slot=$(batch_result_slot_from_log "${BAM_SECONDARY_LOG}" "${SEQ_TWO}")
      [[ -n "${primary_slot}" && "${primary_slot}" == "${secondary_slot}" ]]         || die "Jito BAM fee-config churn did not stay in one slot: primary=${primary_slot:-unknown} secondary=${secondary_slot:-unknown}"
    fi
    ;;
  bam_fee_config_refresh_priority_commit|bam_fee_config_commission_refresh_priority_commit)
    expected_fee_config_pubkey="${RECIPIENT_ONE}"
    if [[ "${MODE_VALUE}" == "bam_fee_config_commission_refresh_priority_commit" ]]; then
      expected_fee_config_pubkey="${RECIPIENT_TWO}"
    fi
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}"       || die "jito-agave did not commit the first priority-fee batch before config refresh"
    wait_for_pattern "${BAM_LOG}" "scripted config_update conn=1 .*prio_fee_recipient_pubkey=${expected_fee_config_pubkey}.*commission_bps=700" "${TIMEOUT_SECS}"       || die "jito scenario did not mutate live BamConfig"
    wait_for_pattern "${BAM_LOG}" "GetBuilderConfig: .*prio_fee_recipient_pubkey=${expected_fee_config_pubkey}.*commission_bps=700" "${TIMEOUT_SECS}"       || die "jito-agave did not refresh GetBuilderConfig after scripted config update"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed txns=1" "${TIMEOUT_SECS}"       || die "jito-agave did not commit the second priority-fee batch after config refresh"
    wait_for_pattern "${BAM_LOG}" 'scripted wait_inbound satisfied conn=1 kind=committed_batch observed=2' "${TIMEOUT_SECS}"       || die "jito scenario did not observe both config-refresh commits"
    ;;
  bam_fee_config_refresh_queue_burst|bam_fee_config_refresh_source_mix_queue_burst|bam_fee_config_midqueue_refresh|bam_fee_config_midqueue_source_mix_queue_burst|bam_fee_config_midqueue_source_mix_multi_reconnect)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the initial priority-fee batch before config refresh"
    wait_for_pattern "${BAM_LOG}" 'scripted config_update conn=1 .*prio_fee_recipient_pubkey=' "${TIMEOUT_SECS}" \
      || die "jito fee-refresh queue scenario did not mutate live BamConfig"
    wait_for_pattern "${BAM_LOG}" "GetBuilderConfig: .*prio_fee_recipient_pubkey=${RECIPIENT_ONE}" "${TIMEOUT_SECS}" \
      || die "jito-agave did not refresh GetBuilderConfig before fee-refresh queue pressure"
    if [[ "${CLOSE_AFTER_RESULTS:-}" =~ ^[0-9]+$ && "${CLOSE_AFTER_RESULTS}" -gt 0 ]]; then
      wait_for_pattern "${BAM_LOG}" "scripted wait_inbound satisfied conn=1 kind=batch_result observed=[0-9]+" "${TIMEOUT_SECS}" \
        || die "jito fee-refresh queue scenario did not observe the configured pre-close result prefix"
    fi
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for ${MODE_VALUE}"
    if is_queue_multi_reconnect_mode "${MODE_VALUE}"; then
      wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=3' "${TIMEOUT_SECS}" \
        || die "jito-agave did not reconnect to conn=3 for ${MODE_VALUE}"
      wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
        || die "${MODE_VALUE} target did not resume scripted events on conn=2"
      wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=3 resuming at event' "${TIMEOUT_SECS}" \
        || die "${MODE_VALUE} target did not resume scripted events on conn=3"
    else
      wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 replay disabled; not replaying' "${TIMEOUT_SECS}" \
        || die "${MODE_VALUE} target did not keep replay disabled on reconnect"
    fi
    if is_queue_source_mix_mode "${MODE_VALUE}"; then
      wait_for_pattern "${LOG_DIR}/normal_tpu.log" "sending 1 transactions to 127\\.0\\.0\\.1:" "${TIMEOUT_SECS}" \
        || die "normal TPU packet was not sent during ${MODE_VALUE}"
    fi
    burst_tail_timeout="${TIMEOUT_SECS}"
    if [[ "${BURST_TAIL_RESULT_TIMEOUT_SECS}" =~ ^[0-9]+$ ]] && (( BURST_TAIL_RESULT_TIMEOUT_SECS < burst_tail_timeout )); then
      burst_tail_timeout="${BURST_TAIL_RESULT_TIMEOUT_SECS}"
    fi
    if ! wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${PARTIAL_DRAIN_LAST_SEQ} status=" "${burst_tail_timeout}"; then
      echo "warning: jito-agave did not return a terminal last ${MODE_VALUE} batch result; preserving incomplete outcome for diff"
    fi
    ;;
  durable_nonce_commit)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the durable-nonce batch"
    wait_for_pattern "${BAM_LOG}" "scripted wait_inbound satisfied conn=1 kind=committed_batch observed=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not observe the durable-nonce committed batch"
    ;;
  durable_nonce_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave durable-nonce reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for durable-nonce result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave durable-nonce reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the durable-nonce reconnect batch"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal durable-nonce reconnect results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  durable_nonce_replay_after_reconnect)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1 conn=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first durable-nonce replay batch"
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave durable-nonce replay scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect before durable-nonce replay"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave durable-nonce replay scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=not_committed" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the durable-nonce replay"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=(${SEQ_ONE}|${SEQ_TWO}) status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "2" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal durable-nonce replay results; expected exactly two"
    ;;
  durable_nonce_wrong_authority)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=transaction_error index=0 detail=BLOCKHASH_NOT_FOUND" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the wrong-authority durable-nonce batch as BLOCKHASH_NOT_FOUND"
    wait_for_pattern "${BAM_LOG}" "scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not observe the wrong-authority durable-nonce rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal wrong-authority durable-nonce results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  durable_nonce_wrong_authority_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave wrong-authority durable-nonce reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for wrong-authority durable-nonce result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave wrong-authority durable-nonce reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=transaction_error index=0 detail=BLOCKHASH_NOT_FOUND" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the wrong-authority durable-nonce reconnect batch as BLOCKHASH_NOT_FOUND"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal wrong-authority durable-nonce reconnect results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  valid_alt_commit)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the finalized valid-ALT v0 transfer batch"
    wait_for_pattern "${BAM_LOG}" "scripted wait_inbound satisfied conn=1 kind=committed_batch observed=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not observe the valid-ALT committed batch"
    ;;
  invalid_alt_missing_table)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the invalid-ALT missing-table batch with SANITIZE_ERROR"
    wait_for_pattern "${BAM_LOG}" "scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not observe the invalid-ALT not_committed batch"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal invalid-ALT results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  invalid_alt_missing_table_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave invalid-ALT reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for invalid-ALT result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave invalid-ALT reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the invalid-ALT reconnect batch with SANITIZE_ERROR"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal invalid-ALT reconnect results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  source_mix_bam_tpu|source_mix_duplicate_tpu_after_bam)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the BAM half of ${MODE_VALUE}"
    if [[ "${MODE_VALUE}" == "source_mix_duplicate_tpu_after_bam" ]]; then
      tpu_packet="${TARGET_PACKET_FILE}"
    else
      tpu_packet="${TARGET_PACKET_TWO_FILE}"
    fi
    send_normal_tpu_packet_after_bam_commit \
      "${tpu_packet}" \
      "${BAM_LOG}" \
      "${FD_LOG}" \
      "${SEQ_ONE}" \
      "${LOG_DIR}/normal_tpu.log"
    sleep 3
    if grep -q "scheduler<-validator batch_result seq_id=${SEQ_TWO}" "${BAM_LOG}" 2>/dev/null; then
      die "normal TPU packet unexpectedly produced a BAM batch result for seq_id=${SEQ_TWO}"
    fi
    ;;
  source_mix_precommit)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the BAM half of source_mix_precommit"
    sleep 3
    if grep -q "scheduler<-validator batch_result seq_id=${SEQ_TWO}" "${BAM_LOG}" 2>/dev/null; then
      die "normal TPU packet unexpectedly produced a BAM batch result for seq_id=${SEQ_TWO}"
    fi
    ;;
  source_mix_atomic_revert_precommit)
    wait_for_pattern "${LOG_DIR}/normal_tpu.log" "sending 1 transactions to 127\.0\.0\.1:" "${TIMEOUT_SECS}" \
      || die "normal TPU packet was not sent before ${MODE_VALUE}"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=transaction_error index=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected atomic source-mix rejection"
    sleep 3
    if grep -q "scheduler<-validator batch_result seq_id=${SEQ_TWO}" "${BAM_LOG}" 2>/dev/null; then
      die "normal TPU packet unexpectedly produced a BAM batch result for seq_id=${SEQ_TWO}"
    fi
    ;;
  disable_enable_tpu_release)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1 conn=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first BAM batch before disable"
    wait_for_pattern "${BAM_LOG}" 'scheduler stream closed by validator conn=1' "${TIMEOUT_SECS}" \
      || die "jito-agave did not close the scheduler stream after BAM disable"
    wait_for_pattern "${LOG_DIR}/normal_tpu.log" "sending 1 transactions to 127\\.0\\.0\\.1:" "${TIMEOUT_SECS}" \
      || die "normal TPU packet was not sent while BAM was disabled"
    wait_for_pattern "${OPERATOR_LOG}" '^operator: set_bam --disable' "${TIMEOUT_SECS}" \
      || die "jito operator did not issue BAM disable for TPU release"
    wait_for_pattern "${OPERATOR_LOG}" '^operator: set_bam --enable' "${TIMEOUT_SECS}" \
      || die "jito operator did not issue BAM enable after TPU release"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect after BAM re-enable"
    if grep -q "scheduler<-validator batch_result seq_id=${SEQ_TWO}" "${BAM_LOG}" 2>/dev/null; then
      die "normal TPU release packet unexpectedly produced a BAM batch result for seq_id=${SEQ_TWO}"
    fi
    ;;
  unique_after_reconnect|seq_id_wrap_sequence)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the second unique batch seq_id=${SEQ_TWO}"
    ;;
  seq_id_wrap_conflicting_spend_multi_batch)
    wait_for_pattern "${BAM_LOG}" "scripted send_multi_batch seq_ids=\\[${SEQ_TWO}, ${SEQ_ONE}\\] packet_counts=\\[1, 1\\]" "${TIMEOUT_SECS}" \
      || die "jito-agave scenario did not emit the wrap-boundary conflicting-spend request"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first-arriving wrap-boundary batch"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result_tx seq_id=${SEQ_TWO} tx_index=0 execution_success=true" "${TIMEOUT_SECS}" \
      || die "jito-agave did not execute the first-arriving wrap-boundary transaction successfully"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the second conflicting batch as a non-revert committed batch"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result_tx seq_id=${SEQ_ONE} tx_index=0 execution_success=false" "${TIMEOUT_SECS}" \
      || die "jito-agave did not mark the second conflicting transaction as failed after the first spend"
    wait_for_pattern "${BAM_LOG}" 'scripted wait_inbound satisfied conn=1 kind=batch_result observed=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not observe two terminal conflicting-spend batch results"
    ;;
  seq_id_out_of_order_multi_batch|seq_id_wrap_out_of_order_multi_batch)
    wait_for_pattern "${BAM_LOG}" "scripted send_multi_batch seq_ids=\\[${SEQ_TWO}, ${SEQ_ONE}\\] packet_counts=\\[1, 1\\]" "${TIMEOUT_SECS}" \
      || die "jito-agave scenario did not emit the descending-seq multi-batch request"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the lower-seq batch"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the higher-seq batch"
    ;;
  seq_collision_same_conn)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first same-seq payload"
    wait_for_pattern "${BAM_LOG}" 'scripted wait_inbound satisfied conn=1 kind=batch_result observed=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not return two terminal same-seq batch results"
    ;;
  seq_collision_reconnect)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first same-seq payload before reconnect"
    wait_for_pattern "${BAM_LOG}" 'scheduler stream closed by validator conn=1' "${TIMEOUT_SECS}" \
      || die "jito-agave did not close the first scheduler stream for duplicate-seq reconnect"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for duplicate-seq replay"
    wait_for_pattern "${BAM_LOG}" 'scripted wait_inbound satisfied conn=2 kind=batch_result observed=1' "${TIMEOUT_SECS}" \
      || die "jito-agave did not return a terminal same-seq result after reconnect"
    ;;
  disable_enable_unique_after_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scheduler stream closed by validator conn=1' "${TIMEOUT_SECS}" \
      || die "jito-agave did not close the first scheduler stream after BAM disable"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect after BAM re-enable"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the second unique batch after BAM re-enable"
    wait_for_pattern "${OPERATOR_LOG}" '^operator: set_bam --disable' "${TIMEOUT_SECS}" \
      || die "jito operator did not issue BAM disable"
    wait_for_pattern "${OPERATOR_LOG}" '^operator: set_bam --enable' "${TIMEOUT_SECS}" \
      || die "jito operator did not issue BAM enable"
    ;;
  url_churn_unique_after_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scheduler stream closed by validator conn=1' "${TIMEOUT_SECS}" \
      || die "jito-agave did not close the first scheduler stream after bad BAM URL"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect after BAM URL restore"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the second unique batch after BAM URL restore"
    wait_for_pattern "${OPERATOR_LOG}" "^operator: set_bam --url ${BAM_BAD_URL_RE}" "${TIMEOUT_SECS}" \
      || die "jito operator did not issue the temporary bad BAM URL"
    wait_for_pattern "${OPERATOR_LOG}" "^operator: set_bam --url ${BAM_URL_RE}" "${TIMEOUT_SECS}" \
      || die "jito operator did not restore the BAM URL"
    ;;
  mixed_multi_batch)
    wait_for_pattern "${BAM_LOG}" 'scripted wait_inbound satisfied conn=1 kind=committed_batch observed=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not return two committed mixed-batch results"
    wait_for_pattern "${BAM_LOG}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' "${TIMEOUT_SECS}" \
      || die "jito-agave did not return one replay rejection in the mixed-batch results"
    ;;
  mixed_empty_multi_batch)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first valid batch in the valid-empty-valid message"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=$((SEQ_ONE + 1)) status=not_committed reason=deserialization_error index=0 detail=EMPTY" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the empty middle batch"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the second valid batch in the valid-empty-valid message"
    ;;
  mixed_malformed_multi_batch)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first valid batch in the valid-malformed-valid message"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=$((SEQ_ONE + 1)) status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the malformed middle batch"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the second valid batch in the valid-malformed-valid message"
    ;;
  mixed_bad_signature_multi_batch)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first valid batch in the valid-bad-signature-valid message"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=$((SEQ_ONE + 1)) status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the bad-signature middle batch"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the second valid batch in the valid-bad-signature-valid message"
    ;;
  mixed_bad_signature_reconnect)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=$((SEQ_ONE + 1)) status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR conn=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the bad-signature middle batch before reconnect"
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave mixed bad-signature reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for mixed bad-signature reconnect"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 replay disabled; not replaying' "${TIMEOUT_SECS}" \
      || die "jito-agave mixed bad-signature reconnect did not keep replay disabled on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1 conn=[12]" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first valid batch across mixed bad-signature reconnect"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed txns=1 conn=[12]" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the second valid batch across mixed bad-signature reconnect"
    ;;
  mixed_stale_multi_batch)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first valid batch in the valid-stale-valid message"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=$((SEQ_ONE + 1)) status=not_committed reason=scheduling_error detail=OUTSIDE_LEADER_SLOT" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the stale middle batch"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the second valid batch in the valid-stale-valid message"
    ;;
  mixed_stale_reconnect)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=$((SEQ_ONE + 1)) status=not_committed reason=scheduling_error detail=OUTSIDE_LEADER_SLOT conn=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the stale middle batch before reconnect"
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave mixed stale reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for mixed stale reconnect"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 replay disabled; not replaying' "${TIMEOUT_SECS}" \
      || die "jito-agave mixed stale reconnect did not keep replay disabled on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1 conn=[12]" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first valid batch across mixed stale reconnect"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed txns=1 conn=[12]" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the second valid batch across mixed stale reconnect"
    ;;
  mixed_terminal_producers_reconnect)
    wait_for_pattern "${BAM_LOG}" "scripted send_multi_batch seq_ids=\\[${SEQ_ONE}, $((SEQ_ONE + 1)), $((SEQ_ONE + 2)), $((SEQ_ONE + 3)), ${SEQ_TWO}\\] packet_counts=\\[1, 1, 1, 1, 1\\]" "${TIMEOUT_SECS}" \
      || die "jito-agave did not emit the mixed terminal-producer multi-batch request"
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave mixed terminal-producer reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for mixed terminal-producer reconnect"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 replay disabled; not replaying' "${TIMEOUT_SECS}" \
      || die "jito-agave mixed terminal-producer reconnect did not keep replay disabled on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1 conn=[12]" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first valid batch across mixed terminal-producer reconnect"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=$((SEQ_ONE + 1)) status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR conn=[12]" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the malformed batch across mixed terminal-producer reconnect"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=$((SEQ_ONE + 2)) status=not_committed reason=scheduling_error detail=OUTSIDE_LEADER_SLOT conn=[12]" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the stale batch across mixed terminal-producer reconnect"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=$((SEQ_ONE + 3)) status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR conn=[12]" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the bad-signature batch across mixed terminal-producer reconnect"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_TWO} status=committed txns=1 conn=[12]" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the second valid batch across mixed terminal-producer reconnect"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=(${SEQ_ONE}|$((SEQ_ONE + 1))|$((SEQ_ONE + 2))|$((SEQ_ONE + 3))|${SEQ_TWO}) status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "5" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal mixed terminal-producer results; expected exactly five"
    ;;
  random_mixed_multi_batch)
    wait_for_random_mixed_results "${SOURCE_RANDOM_MIXED_PATTERN}" "${SEQ_ONE}"
    ;;
  atomic_revert)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected atomic-revert rejection"
    ;;
  atomic_first_overdraft)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed reason=transaction_error index=0" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected atomic first-overdraft rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal atomic first-overdraft results for seq_id=${SEQ_ONE:-1}; expected exactly one"
    ;;
  atomic_first_overdraft_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave atomic first-overdraft reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for atomic first-overdraft result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave atomic first-overdraft reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed reason=transaction_error index=0" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected atomic first-overdraft reconnect rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal atomic first-overdraft reconnect results for seq_id=${SEQ_ONE:-1}; expected exactly one"
    ;;
  atomic_revert_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave atomic-revert reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for atomic-revert result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave atomic-revert reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed reason=transaction_error index=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected atomic-revert reconnect rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal atomic-revert reconnect results for seq_id=${SEQ_ONE:-1}; expected exactly one"
    ;;
  atomic_mid_fail)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed reason=transaction_error index=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected atomic middle-failure rejection"
    ;;
  atomic_mid_fail_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave atomic middle-failure reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for atomic middle-failure result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave atomic middle-failure reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed reason=transaction_error index=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected atomic middle-failure reconnect rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal atomic middle-failure reconnect results for seq_id=${SEQ_ONE:-1}; expected exactly one"
    ;;
  atomic_blockhash_mid_fail)
    if grep -q "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed reason=transaction_error index=1 detail=BLOCKHASH_NOT_FOUND" "${BAM_LOG}" 2>/dev/null; then
      :
    elif grep -q "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed reason=deserialization_error index=1 detail=SANITIZE_ERROR" "${BAM_LOG}" 2>/dev/null; then
      :
    elif grep -q 'scripted wait_inbound timeout conn=1 kind=batch_result observed=0 expected_at_least=1' "${BAM_LOG}" 2>/dev/null; then
      echo "warning: jito-agave atomic_blockhash_mid_fail produced no BAM batch result; preserving incomplete outcome for differential comparison"
    else
      wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed reason=transaction_error index=1 detail=BLOCKHASH_NOT_FOUND|reason=deserialization_error index=1 detail=SANITIZE_ERROR|scripted wait_inbound timeout conn=1 kind=batch_result observed=0 expected_at_least=1" 5 \
        || die "jito-agave did not return the expected atomic blockhash failure or timeout evidence"
    fi
    ;;
  atomic_blockhash_mid_fail_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave atomic-blockhash reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for atomic-blockhash result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave atomic-blockhash reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed reason=transaction_error index=1 detail=BLOCKHASH_NOT_FOUND|scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed reason=deserialization_error index=1 detail=SANITIZE_ERROR" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected atomic-blockhash reconnect rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal atomic-blockhash reconnect results for seq_id=${SEQ_ONE:-1}; expected exactly one"
    ;;
  atomic_resolver_mid_fail)
    if grep -q "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed reason=deserialization_error index=1" "${BAM_LOG}" 2>/dev/null; then
      :
    elif grep -q 'scripted wait_inbound timeout conn=1 kind=batch_result observed=0 expected_at_least=1' "${BAM_LOG}" 2>/dev/null; then
      echo "warning: jito-agave atomic_resolver_mid_fail produced no BAM batch result; preserving incomplete outcome for differential comparison"
    else
      wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed reason=deserialization_error index=1|scripted wait_inbound timeout conn=1 kind=batch_result observed=0 expected_at_least=1" 5 \
        || die "jito-agave did not return the expected atomic resolver failure or timeout evidence"
    fi
    ;;
  atomic_resolver_mid_fail_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave atomic-resolver reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for atomic-resolver result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave atomic-resolver reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed reason=deserialization_error index=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected atomic-resolver reconnect rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal atomic-resolver reconnect results for seq_id=${SEQ_ONE:-1}; expected exactly one"
    ;;
  atomic_duplicate_sig_mid_fail)
    if grep -q "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed" "${BAM_LOG}" 2>/dev/null; then
      terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=" "${BAM_LOG}" 2>/dev/null || true)
      [[ "${terminal_result_count}" == "1" ]] \
        || die "jito-agave returned ${terminal_result_count} terminal atomic duplicate-signature results for seq_id=${SEQ_ONE:-1}; expected exactly one"
    elif grep -q 'scripted wait_inbound timeout conn=1 kind=batch_result observed=0 expected_at_least=1' "${BAM_LOG}" 2>/dev/null; then
      echo "warning: jito-agave atomic_duplicate_sig_mid_fail produced no BAM batch result; preserving incomplete outcome for differential comparison"
    else
      wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed|scripted wait_inbound timeout conn=1 kind=batch_result observed=0 expected_at_least=1" 5 \
        || die "jito-agave did not return the expected atomic duplicate-signature failure or timeout evidence"
    fi
    ;;
  atomic_duplicate_sig_mid_fail_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave atomic duplicate-signature reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for atomic duplicate-signature result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave atomic duplicate-signature reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=not_committed .*index=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected atomic duplicate-signature reconnect rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE:-1} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal atomic duplicate-signature reconnect results for seq_id=${SEQ_ONE:-1}; expected exactly one"
    ;;
  queue_burst_reconnect)
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for ${MODE_VALUE} replay"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 replay disabled; not replaying' "${TIMEOUT_SECS}" \
      || die "${MODE_VALUE} target did not keep replay disabled on reconnect"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${PARTIAL_DRAIN_LAST_SEQ} status=committed" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the last ${MODE_VALUE} batch result"
    ;;
  queue_burst_multi_reconnect|source_mix_queue_burst_multi_reconnect)
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect to conn=2 for ${MODE_VALUE}"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=3' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect to conn=3 for ${MODE_VALUE}"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "${MODE_VALUE} target did not resume scripted events on conn=2"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=3 resuming at event' "${TIMEOUT_SECS}" \
      || die "${MODE_VALUE} target did not resume scripted events on conn=3"
    wait_for_pattern "${BAM_LOG}" 'scripted wait_inbound satisfied conn=2 kind=batch_result observed=' "${TIMEOUT_SECS}" \
      || die "${MODE_VALUE} target did not observe a durable conn=2 result before the second close"
    burst_tail_timeout="${TIMEOUT_SECS}"
    if [[ "${BURST_TAIL_RESULT_TIMEOUT_SECS}" =~ ^[0-9]+$ ]] && (( BURST_TAIL_RESULT_TIMEOUT_SECS < burst_tail_timeout )); then
      burst_tail_timeout="${BURST_TAIL_RESULT_TIMEOUT_SECS}"
    fi
    if ! wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${PARTIAL_DRAIN_LAST_SEQ} status=" "${burst_tail_timeout}"; then
      echo "warning: jito-agave did not return a terminal last ${MODE_VALUE} batch result; preserving incomplete outcome for diff"
    fi
    if [[ "${MODE_VALUE}" == "source_mix_queue_burst_multi_reconnect" ]]; then
      wait_for_pattern "${LOG_DIR}/normal_tpu.log" "sending 1 transactions to 127\\.0\\.0\\.1:" "${TIMEOUT_SECS}" \
        || die "${MODE_VALUE} target did not inject the normal TPU packet"
    fi
    ;;
  partial_drain_reconnect|queue_burst64_reconnect|queue_burst64_leader_plus1_reconnect|schedule_boundary_jitter|queue_reconnect_timing_jitter|queue_burst128_reconnect|queue_burst256_reconnect|queue_burst512_reconnect|queue_burst_leader_reconnect|queue_burst64_leader_reconnect|bam_fee_queue_burst_reconnect|bam_fee_source_mix_queue_burst_reconnect|source_mix_queue_burst_reconnect)
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for ${MODE_VALUE} replay"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 replay disabled; not replaying' "${TIMEOUT_SECS}" \
      || die "${MODE_VALUE} target did not keep replay disabled on reconnect"
    burst_tail_timeout="${TIMEOUT_SECS}"
    if [[ "${BURST_TAIL_RESULT_TIMEOUT_SECS}" =~ ^[0-9]+$ ]] && (( BURST_TAIL_RESULT_TIMEOUT_SECS < burst_tail_timeout )); then
      burst_tail_timeout="${BURST_TAIL_RESULT_TIMEOUT_SECS}"
    fi
    if ! wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${PARTIAL_DRAIN_LAST_SEQ} status=" "${burst_tail_timeout}"; then
      echo "warning: jito-agave did not return a terminal last ${MODE_VALUE} batch result; preserving incomplete outcome for diff"
    fi
    ;;
  disable_enable_queue_burst_reconnect)
    wait_for_pattern "${OPERATOR_LOG}" '^operator: set_bam --disable' "${TIMEOUT_SECS}" \
      || die "jito operator did not issue BAM disable during queue pressure"
    wait_for_pattern "${BAM_LOG}" 'scheduler stream closed by validator conn=1' "${TIMEOUT_SECS}" \
      || die "jito-agave did not close the scheduler stream after queue BAM disable"
    wait_for_pattern "${OPERATOR_LOG}" '^operator: set_bam --enable' "${TIMEOUT_SECS}" \
      || die "jito operator did not re-enable BAM during queue pressure"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect after queue BAM re-enable"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 replay disabled; not replaying' "${TIMEOUT_SECS}" \
      || die "${MODE_VALUE} target did not keep replay disabled on reconnect"
    burst_tail_timeout="${TIMEOUT_SECS}"
    if [[ "${BURST_TAIL_RESULT_TIMEOUT_SECS}" =~ ^[0-9]+$ ]] && (( BURST_TAIL_RESULT_TIMEOUT_SECS < burst_tail_timeout )); then
      burst_tail_timeout="${BURST_TAIL_RESULT_TIMEOUT_SECS}"
    fi
    if ! wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${PARTIAL_DRAIN_LAST_SEQ} status=" "${burst_tail_timeout}"; then
      echo "warning: jito-agave did not return a terminal last ${MODE_VALUE} batch result; preserving incomplete outcome for diff"
    fi
    ;;
  vote_reject_once)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=deserialization_error index=0 detail=VOTE_TRANSACTION_FAILURE" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected vote rejection"
    ;;
  vote_reject_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave vote reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for vote rejection result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave vote reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=deserialization_error index=0 detail=VOTE_TRANSACTION_FAILURE" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected vote reconnect rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal vote reconnect results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  raw_kunorpus_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave raw-kunorpus reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for raw-kunorpus result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave raw-kunorpus reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return a terminal raw-kunorpus reconnect result"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal raw-kunorpus reconnect results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  stale_slot_reject)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=scheduling_error detail=OUTSIDE_LEADER_SLOT" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected stale-slot scheduling rejection"
    ;;
  stale_slot_reject_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave stale-slot reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for stale-slot result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave stale-slot reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=scheduling_error detail=OUTSIDE_LEADER_SLOT" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected stale-slot reconnect rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal stale-slot reconnect results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  empty_batch_reject)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=deserialization_error index=0 detail=EMPTY" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected empty-batch rejection"
    ;;
  empty_batch_reject_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave empty-batch reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for empty-batch result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave empty-batch reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=deserialization_error index=0 detail=EMPTY" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected empty-batch reconnect rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal empty-batch reconnect results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  malformed_first_atomic)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the malformed-first atomic batch"
    ;;
  malformed_first_atomic_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave malformed-first reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for malformed-first result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave malformed-first reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected malformed-first reconnect rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal malformed-first reconnect results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  malformed_tail_atomic)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the malformed-tail atomic batch"
    ;;
  malformed_tail_atomic_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave malformed-tail reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for malformed-tail result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave malformed-tail reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=deserialization_error index=1 detail=SANITIZE_ERROR" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected malformed-tail reconnect rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal malformed-tail reconnect results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  bad_signature_first_atomic)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the bad-signature-first atomic batch"
    ;;
  bad_signature_first_atomic_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave bad-signature-first reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for bad-signature-first result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave bad-signature-first reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected bad-signature-first reconnect rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal bad-signature-first reconnect results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  bad_signature_tail_atomic)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed" "${TIMEOUT_SECS}" \
      || die "jito-agave did not reject the bad-signature-tail atomic batch"
    ;;
  bad_signature_tail_atomic_reconnect)
    wait_for_pattern "${BAM_LOG}" 'scripted close_stream' "${TIMEOUT_SECS}" \
      || die "jito-agave bad-signature-tail reconnect scenario did not close conn=1"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for bad-signature-tail result drain"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "jito-agave bad-signature-tail reconnect scenario did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=not_committed reason=deserialization_error index=1 detail=SANITIZE_ERROR" "${TIMEOUT_SECS}" \
      || die "jito-agave did not return the expected bad-signature-tail reconnect rejection"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "1" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal bad-signature-tail reconnect results for seq_id=${SEQ_ONE}; expected exactly one"
    ;;
  duplicate_seq_split)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=5" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first duplicate-seq split fragment"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the second duplicate-seq split fragment"
    ;;
  duplicate_seq_split_reconnect)
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=5 conn=1" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the first duplicate-seq reconnect fragment"
    wait_for_pattern "${BAM_LOG}" 'InitSchedulerStream:.*conn=2' "${TIMEOUT_SECS}" \
      || die "jito-agave did not reconnect for duplicate-seq split reconnect"
    wait_for_pattern "${BAM_LOG}" 'scripted scenario conn=2 resuming at event' "${TIMEOUT_SECS}" \
      || die "duplicate-seq split reconnect target did not resume on conn=2"
    wait_for_pattern "${BAM_LOG}" "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=committed txns=1 conn=2" "${TIMEOUT_SECS}" \
      || die "jito-agave did not commit the second duplicate-seq reconnect fragment"
    terminal_result_count=$(grep -cE "scheduler<-validator batch_result seq_id=${SEQ_ONE} status=" "${BAM_LOG}" 2>/dev/null || true)
    [[ "${terminal_result_count}" == "2" ]] \
      || die "jito-agave returned ${terminal_result_count} terminal duplicate-seq reconnect results for seq_id=${SEQ_ONE}; expected exactly two"
    ;;
esac

if [[ -n "${OPERATOR_PID:-}" ]]; then
  if ! wait "${OPERATOR_PID}"; then
    OPERATOR_PID=""
    die "jito operator runner failed; see ${OPERATOR_LOG}"
  fi
  OPERATOR_PID=""
fi

sleep 2
if [[ -n "${PAYER}" && "${PAYER}" != "n/a" ]]; then
  PAYER_OBSERVED=$(rpc_balance "${PAYER}" 2>/dev/null || true)
fi
recipient_one_observed=""
recipient_two_observed=""
recipient_one_owner_observed=""
recipient_two_owner_observed=""
recipient_one_space_observed=""
recipient_two_space_observed=""
fee_churn_min_one=""
fee_churn_min_two=""
fee_commission_min_two=""
fee_queue_min_two=""
if is_fee_config_two_recipient_mode "${MODE_VALUE}"; then
  CHURN_LAMPORTS=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*lamports=\([0-9][0-9]*\).*/\1/p')
  [[ "${CHURN_LAMPORTS}" =~ ^[0-9]+$ ]] || die "failed to derive fee-churn transfer lamports"
  fee_churn_min_one=$((recipient_one_initial + CHURN_LAMPORTS))
  fee_churn_min_two=$((recipient_two_initial + CHURN_LAMPORTS))
fi
if is_fee_config_commission_only_mode "${MODE_VALUE}"; then
  CHURN_LAMPORTS=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*lamports=\([0-9][0-9]*\).*/\1/p')
  [[ "${CHURN_LAMPORTS}" =~ ^[0-9]+$ ]] || die "failed to derive fee commission-refresh transfer lamports"
  fee_commission_min_two=$((recipient_two_initial + CHURN_LAMPORTS))
fi
if is_fee_queue_burst_mode "${MODE_VALUE}"; then
  QUEUE_LAMPORTS=$(printf '%s\n' "${SOURCE_INPUT_NOTE}" | sed -n 's/.*lamports=\([0-9][0-9]*\).*/\1/p')
  [[ "${QUEUE_LAMPORTS}" =~ ^[0-9]+$ ]] || die "failed to derive fee queue transfer lamports"
  fee_queue_min_two=$((recipient_two_initial + QUEUE_LAMPORTS))
fi
if [[ -n "${RECIPIENT_ONE}" && "${RECIPIENT_ONE}" != "n/a" ]]; then
  if [[ -n "${fee_churn_min_one}" ]]; then
    recipient_one_observed=$(observe_balance_at_least_or_current "${RECIPIENT_ONE}" "${fee_churn_min_one}" "${BALANCE_OBSERVE_TIMEOUT_SECS}")
  else
    recipient_one_observed=$(rpc_balance "${RECIPIENT_ONE}" 2>/dev/null || true)
  fi
  recipient_one_owner_observed=$(rpc_account_owner "${RECIPIENT_ONE}" 2>/dev/null || true)
  recipient_one_space_observed=$(rpc_account_space "${RECIPIENT_ONE}" 2>/dev/null || true)
fi
if [[ -n "${RECIPIENT_TWO}" && "${RECIPIENT_TWO}" != "n/a" ]]; then
  if [[ -n "${fee_churn_min_two}" ]]; then
    recipient_two_observed=$(observe_balance_at_least_or_current "${RECIPIENT_TWO}" "${fee_churn_min_two}" "${BALANCE_OBSERVE_TIMEOUT_SECS}")
  elif [[ -n "${fee_commission_min_two}" ]]; then
    recipient_two_observed=$(observe_balance_at_least_or_current "${RECIPIENT_TWO}" "${fee_commission_min_two}" "${BALANCE_OBSERVE_TIMEOUT_SECS}")
  elif [[ -n "${fee_queue_min_two}" ]]; then
    recipient_two_observed=$(observe_balance_at_least_or_current "${RECIPIENT_TWO}" "${fee_queue_min_two}" "${BALANCE_OBSERVE_TIMEOUT_SECS}")
  elif [[ "${MODE_VALUE}" == "disable_enable_tpu_release" && "${recipient_two_expected}" =~ ^[0-9]+$ ]]; then
    recipient_two_observed=$(observe_balance_at_least_or_current "${RECIPIENT_TWO}" "${recipient_two_expected}" "${BALANCE_OBSERVE_TIMEOUT_SECS}")
  else
    recipient_two_observed=$(rpc_balance "${RECIPIENT_TWO}" 2>/dev/null || true)
  fi
  recipient_two_owner_observed=$(rpc_account_owner "${RECIPIENT_TWO}" 2>/dev/null || true)
  recipient_two_space_observed=$(rpc_account_space "${RECIPIENT_TWO}" 2>/dev/null || true)
fi
if [[ "${MODE_VALUE}" == "fee_only_commit" || "${MODE_VALUE}" == "fee_only_reconnect" ]]; then
  [[ "${recipient_one_observed}" == "${recipient_one_initial}" ]] \
    || die "${MODE_VALUE} recipient ${RECIPIENT_ONE} changed unexpectedly: got ${recipient_one_observed:-unknown}, expected ${recipient_one_initial}"
  [[ "${recipient_two_observed}" == "${recipient_two_initial}" ]] \
    || die "${MODE_VALUE} second recipient ${RECIPIENT_TWO} changed unexpectedly: got ${recipient_two_observed:-unknown}, expected ${recipient_two_initial}"
  [[ "${PAYER_INITIAL}" =~ ^[0-9]+$ && "${PAYER_OBSERVED}" =~ ^[0-9]+$ && "${PAYER_OBSERVED}" -lt "${PAYER_INITIAL}" ]] \
    || die "${MODE_VALUE} payer ${PAYER} was not debited: got ${PAYER_OBSERVED:-unknown}, initial ${PAYER_INITIAL:-unknown}"
fi
if [[ "${MODE_VALUE}" == "bam_fee_priority_replay_after_reconnect" && -n "${BAM_FEE_RECIPIENT}" ]]; then
  BAM_FEE_RECIPIENT_OBSERVED="${recipient_one_observed}"
elif [[ -n "${BAM_FEE_RECIPIENT}" ]]; then
  BAM_FEE_RECIPIENT_OBSERVED="${recipient_two_observed}"
fi
if is_fee_queue_burst_mode "${MODE_VALUE}"; then
  BAM_FEE_RECIPIENT_OBSERVED="${recipient_two_observed}"
  if ! [[ "${recipient_two_observed}" =~ ^[0-9]+$ && "${recipient_two_observed}" -ge "${fee_queue_min_two}" ]]; then
    echo "warning: Jito BAM fee recipient ${RECIPIENT_TWO} did not reach queue transfer-inclusive balance: got ${recipient_two_observed:-unknown}, minimum ${fee_queue_min_two}; preserving outcome for diff"
  fi
fi
if is_fee_config_commission_only_mode "${MODE_VALUE}"; then
  BAM_FEE_RECIPIENT_OBSERVED="${recipient_two_observed}"
  if ! [[ "${recipient_two_observed}" =~ ^[0-9]+$ && "${recipient_two_observed}" -ge "${fee_commission_min_two}" ]]; then
    echo "warning: Jito BAM fee recipient ${RECIPIENT_TWO} did not reach commission-refresh transfer-inclusive balance: got ${recipient_two_observed:-unknown}, minimum ${fee_commission_min_two}; preserving outcome for diff"
  fi
fi
if is_fee_config_two_recipient_mode "${MODE_VALUE}"; then
  BAM_FEE_RECIPIENT_OBSERVED="${recipient_two_observed}"
  BAM_FEE_RECIPIENT_SECOND_OBSERVED="${recipient_one_observed}"
  if ! [[ "${recipient_one_observed}" =~ ^[0-9]+$ && "${recipient_one_observed}" -ge "${fee_churn_min_one}" ]]; then
    echo "warning: alternate Jito BAM fee recipient ${RECIPIENT_ONE} did not reach transfer-inclusive balance: got ${recipient_one_observed:-unknown}, minimum ${fee_churn_min_one}; preserving outcome for diff"
  fi
  if ! [[ "${recipient_two_observed}" =~ ^[0-9]+$ && "${recipient_two_observed}" -ge "${fee_churn_min_two}" ]]; then
    echo "warning: initial Jito BAM fee recipient ${RECIPIENT_TWO} did not reach transfer-inclusive balance: got ${recipient_two_observed:-unknown}, minimum ${fee_churn_min_two}; preserving outcome for diff"
  fi
fi
END_SLOT=$(rpc_slot 2>/dev/null || printf "${START_SLOT}")
CAPTURE_END_SLOT="${END_SLOT}"
if [[ "${START_SLOT}" =~ ^[0-9]+$ && "${END_SLOT}" =~ ^[0-9]+$ && "${CAPTURE_BLOCK_SLOT_LIMIT}" =~ ^[0-9]+$ ]] \
    && (( CAPTURE_BLOCK_SLOT_LIMIT > 0 )) \
    && (( END_SLOT > START_SLOT + CAPTURE_BLOCK_SLOT_LIMIT )); then
  CAPTURE_END_SLOT=$((START_SLOT + CAPTURE_BLOCK_SLOT_LIMIT))
fi
if [[ "${CAPTURE_RPC_EVIDENCE}" == "1" ]]; then
  capture_rpc_blocks "${START_SLOT}" "${CAPTURE_END_SLOT}" "${LOG_DIR}/rpc_blocks.jsonl"
  if [[ "${MODE_VALUE}" == "source_mix_duplicate_tpu_after_bam" || "${MODE_VALUE}" == "source_mix_atomic_revert_precommit" ]]; then
    capture_rpc_signature_statuses "${EFFECTIVE_SCENARIO_FILE}" "${LOG_DIR}/rpc_signature_statuses.jsonl" "${TARGET_PACKET_FILE}"
  elif [[ "${MODE_VALUE}" == "source_mix_bam_tpu" || "${MODE_VALUE}" == "source_mix_precommit" || "${MODE_VALUE}" == "disable_enable_tpu_release" ]]; then
    capture_rpc_signature_statuses "${EFFECTIVE_SCENARIO_FILE}" "${LOG_DIR}/rpc_signature_statuses.jsonl" "${TARGET_PACKET_TWO_FILE}"
  elif is_queue_source_mix_mode "${MODE_VALUE}"; then
    capture_rpc_signature_statuses "${EFFECTIVE_SCENARIO_FILE}" "${LOG_DIR}/rpc_signature_statuses.jsonl" "${TARGET_NORMAL_TPU_PACKET_FILE}"
  else
    capture_rpc_signature_statuses "${EFFECTIVE_SCENARIO_FILE}" "${LOG_DIR}/rpc_signature_statuses.jsonl"
  fi
else
  : >"${LOG_DIR}/rpc_blocks.jsonl"
  : >"${LOG_DIR}/rpc_signature_statuses.jsonl"
fi

	cat > "${LOG_DIR}/summary.txt" <<EOF2
seed=${SEED_VALUE}
mode=${MODE_VALUE}
input_family=${INPUT_FAMILY_VALUE}
start_slot=${START_SLOT}
end_slot=${END_SLOT}
capture_end_slot=${CAPTURE_END_SLOT}
source_summary=${SOURCE_SUMMARY}
scenario_file=${SCENARIO_FILE}
effective_scenario_file=${EFFECTIVE_SCENARIO_FILE}
random_mixed_pattern=${SOURCE_RANDOM_MIXED_PATTERN}
ledger_dir=${LEDGER_DIR}
target_blockhash=${TARGET_BLOCKHASH}
target_packet_file=${TARGET_PACKET_FILE}
target_packet_two_file=${TARGET_PACKET_TWO_FILE}
target_packet_three_file=${TARGET_PACKET_THREE_FILE}
target_packet_batch_file=${TARGET_PACKET_BATCH_FILE}
target_bridge_out=${TARGET_BRIDGE_OUT}
target_bridge_two_out=${TARGET_BRIDGE_TWO_OUT}
target_bridge_three_out=${TARGET_BRIDGE_THREE_OUT}
target_durable_nonce_account=${TARGET_DURABLE_NONCE_ACCOUNT}
target_durable_nonce_hash=${TARGET_DURABLE_NONCE_HASH}
normal_tpu_log=${NORMAL_TPU_LOG}
normal_tpu_packet=${NORMAL_TPU_PACKET}
normal_tpu_dst=${NORMAL_TPU_DST}
normal_tpu_port=${NORMAL_TPU_PORT}
normal_tpu_expected_landed=${NORMAL_TPU_EXPECTED_LANDED}
payer=${PAYER}
payer_initial=${PAYER_INITIAL}
payer_observed=${PAYER_OBSERVED}
recipient_one=${RECIPIENT_ONE}
recipient_one_initial=${recipient_one_initial}
recipient_one_expected=${recipient_one_expected}
recipient_one_observed=${recipient_one_observed}
recipient_one_owner_expected=${RECIPIENT_ONE_OWNER_EXPECTED}
recipient_one_owner_observed=${recipient_one_owner_observed}
recipient_one_space_expected=${RECIPIENT_ONE_SPACE_EXPECTED}
recipient_one_space_observed=${recipient_one_space_observed}
recipient_two=${RECIPIENT_TWO}
recipient_two_initial=${recipient_two_initial}
recipient_two_expected=${recipient_two_expected}
recipient_two_observed=${recipient_two_observed}
recipient_two_owner_expected=${RECIPIENT_TWO_OWNER_EXPECTED}
recipient_two_owner_observed=${recipient_two_owner_observed}
recipient_two_space_expected=${RECIPIENT_TWO_SPACE_EXPECTED}
recipient_two_space_observed=${recipient_two_space_observed}
bam_fee_recipient=${BAM_FEE_RECIPIENT}
bam_fee_recipient_initial=${BAM_FEE_RECIPIENT_INITIAL}
bam_fee_recipient_observed=${BAM_FEE_RECIPIENT_OBSERVED}
bam_fee_recipient_second=${BAM_FEE_RECIPIENT_SECOND}
bam_fee_recipient_second_initial=${BAM_FEE_RECIPIENT_SECOND_INITIAL}
bam_fee_recipient_second_observed=${BAM_FEE_RECIPIENT_SECOND_OBSERVED}
bam_fee_recipient_expected_min_delta=0
bam_fee_commission_bps=${BAM_COMMISSION_BPS}
bam_fee_builder_commission_pct=${BAM_BUILDER_COMMISSION_PCT}
bam_fee_secondary_log=${BAM_SECONDARY_LOG}
capture_rpc_evidence=${CAPTURE_RPC_EVIDENCE}
balance_observe_timeout_secs=${BALANCE_OBSERVE_TIMEOUT_SECS}
EOF2

"${NORMALIZER}" \
  --iter-dir "${LOG_DIR}" \
  --target "${TARGET_NAME}" \
  --runner-kind jito-agave \
  --mode "${MODE_VALUE:-unknown}" \
  --input-family "${INPUT_FAMILY_VALUE:-unknown}"

run_bam_disabled_workload
echo "validated: jito-agave BAM scenario produced a normalized outcome"
echo "artifacts: ${LOG_DIR}"
