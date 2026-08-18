#!/usr/bin/env bash

# Seeded live BAM runner for local fddev.
#
# Each iteration:
# 1. boots a fresh local validator baseline or dispatches to a helper-backed mode;
# 2. chooses one executable or adversarial BAM input family;
# 3. adapts it into one or more local BAM packets when needed;
# 4. runs a scripted BAM scenario against live fddev;
# 5. checks BAM feedback plus state / balance invariants.

set -euo pipefail
IFS=$'\n\t'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)

CONFIG="${ROOT}/contrib/local-bam-compact.toml"
RPC_URL="${RPC_URL:-http://127.0.0.1:8899}"
GUI_URL="${GUI_URL:-}"
METRICS_URL="${METRICS_URL:-http://127.0.0.1:7999/metrics}"
TARGET_NAME="${TARGET_NAME:-fddev}"
FDDEV_BIN="${FDDEV_BIN:-}"
FDCTL_BIN="${FDCTL_BIN:-${ROOT}/build/native/gcc/bin/fdctl}"
RUNNER_KIND="${RUNNER_KIND:-native}"
SOLANA_BIN_DIR="${SOLANA_BIN_DIR:-}"
BAM_BIND="${BAM_BIND:-127.0.0.1:50055}"
BAM_URL="${BAM_URL:-http://127.0.0.1:50055}"
BAM_BAD_URL="${BAM_BAD_URL:-http://127.0.0.1:50056}"
BAM_TPU_PORT="${BAM_TPU_PORT:-9007}"
BAM_TPU_FWD_PORT="${BAM_TPU_FWD_PORT:-9008}"
BAM_SHRED_PORT="${BAM_SHRED_PORT:-9009}"
CHECK_BAM_SHRED="${CHECK_BAM_SHRED:-0}"
BAM_BUILDER_COMMISSION_PCT="${BAM_BUILDER_COMMISSION_PCT:-3}"
BAM_COMMISSION_BPS="${BAM_COMMISSION_BPS:-300}"
BAM_SERVER_TLS_CERT="${BAM_SERVER_TLS_CERT:-}"
BAM_SERVER_TLS_KEY="${BAM_SERVER_TLS_KEY:-}"
LOG_DIR=""
ITERATIONS=1
SEED=1
MODE="random"
MODE_LIST=""
EXTERNAL_SCENARIO_FILE=""
EXTERNAL_OPERATOR_EVENTS_FILE=""
INPUT_FAMILY="synthetic"
KUNORPUS_COUNT=64
KUNORPUS_SEED_WINDOW=16
KUNORPUS_MAX_TRANSFER_LAMPORTS=50000000000000
KUNORPUS_SYSTEM_KIND="any"
GENERATED_BUNDLE_MAX_BATCHES=8
GENERATED_BUNDLE_MAX_PACKETS=5
TIMEOUT_SECS=90
RPC_READY_TIMEOUT_SECS="${RPC_READY_TIMEOUT_SECS:-60}"
RPC_CURL_CONNECT_TIMEOUT_SECS="${RPC_CURL_CONNECT_TIMEOUT_SECS:-2}"
RPC_CURL_MAX_TIME_SECS="${RPC_CURL_MAX_TIME_SECS:-2}"
BALANCE_OBSERVE_TIMEOUT_SECS="${BALANCE_OBSERVE_TIMEOUT_SECS:-10}"
USE_SUDO="${USE_SUDO:-1}"
ALLOW_OPERATOR_PERTURBATIONS=0
ALLOW_SELF_CHECK_FAILURES="${ALLOW_SELF_CHECK_FAILURES:-0}"
LIVE_COVERAGE_DIR="${LIVE_COVERAGE_DIR:-}"
QUEUE_BURST_BATCH_COUNT="${QUEUE_BURST_BATCH_COUNT:-}"
QUEUE_BURST_MAX_SCHEDULE_SLOT="${QUEUE_BURST_MAX_SCHEDULE_SLOT:-}"
QUEUE_BURST_CLOSE_AFTER_RESULTS="${QUEUE_BURST_CLOSE_AFTER_RESULTS:-}"
NORMAL_TPU_BURST_COUNT="${NORMAL_TPU_BURST_COUNT:-1}"
NORMAL_TPU_MATRIX_COUNT="${NORMAL_TPU_MATRIX_COUNT:-0}"
BAM_DISABLED_HOLD_MS="${BAM_DISABLED_HOLD_MS:-6000}"
BAM_DISABLED_WORKLOAD_SCRIPT="${BAM_DISABLED_WORKLOAD_SCRIPT:-}"
FROM_GENESIS_ACCOUNT=0
TO_GENESIS_ACCOUNT_ONE=1
TO_GENESIS_ACCOUNT_TWO=2
SLOT_HASHES_SYSVAR="SysvarS1otHashes111111111111111111111111111"
NODE_KEYPAIR_PATH="${HOME}/.firedancer/fd1/identity.json"
VOTE_KEYPAIR_PATH="${HOME}/.firedancer/fd1/vote-account.json"
AUTHORIZED_VOTER_KEYPAIR_PATH="${HOME}/.firedancer/fd1/identity.json"
RPC_CURL_ARGS=(-s --connect-timeout "${RPC_CURL_CONNECT_TIMEOUT_SECS}" --max-time "${RPC_CURL_MAX_TIME_SECS}")

usage() {
  cat <<EOF2
Usage: $(basename "$0") [OPTIONS]

Options:
  --config PATH                     fddev config path (default: contrib/local-bam-compact.toml)
  --rpc-url URL                     JSON RPC URL (default: http://127.0.0.1:8899)
  --target-name NAME                Label for normalized outcome artifacts
                                    (default: fddev)
  --fddev-bin PATH                  Development validator binary to launch
                                    (default: build/native/gcc/bin/fddev)
  --runner-kind NAME                Normalized runner_kind for native runs
                                    (default: native)
  --solana-bin-dir PATH             Directory containing optional Solana CLI
                                    helpers for modes that need them
  --bam-bind HOST:PORT              BAM test-server bind address
                                    (default: 127.0.0.1:50055)
  --bam-url URL                     BAM URL configured into fddev
                                    (default: http://127.0.0.1:50055)
  --bam-bad-url URL                 Temporary bad BAM URL for URL churn
                                    (default: http://127.0.0.1:50056)
  --bam-tpu-port PORT               TPU port advertised by BAM test server
                                    (default: 9007)
  --bam-tpu-fwd-port PORT           TPU forward port advertised by BAM
                                    (default: 9008)
  --bam-shred-port PORT             Shred port advertised by BAM
                                    (default: 9009)
  --check-bam-shred                 Capture the advertised BAM shred UDP port
                                    and require at least one packet
  --log-dir PATH                    Artifact root (default: mktemp)
  --iterations N                    Iteration count (default: 1)
  --seed N                          Base seed (default: 1)
  --mode NAME                       One of: random, commit_once, replay_same_conn,
                                    replay_after_reconnect, unique_after_reconnect,
                                    seq_id_wrap_sequence,
                                    seq_id_out_of_order_multi_batch,
                                    seq_id_wrap_out_of_order_multi_batch,
                                    seq_id_wrap_conflicting_spend_multi_batch,
                                    partial_drain_reconnect, queue_burst_reconnect,
                                    queue_burst64_reconnect,
                                    queue_burst64_leader_plus1_reconnect,
                                    schedule_boundary_jitter,
                                    queue_reconnect_timing_jitter,
                                    queue_burst_multi_reconnect,
                                    queue_burst128_reconnect,
                                    queue_burst256_reconnect,
                                    queue_burst512_reconnect,
                                    queue_burst_leader_reconnect,
                                    queue_burst64_leader_reconnect,
                                    source_mix_bam_tpu,
                                    source_mix_precommit,
                                    source_mix_atomic_revert_precommit,
                                    source_mix_duplicate_tpu_after_bam,
                                    disable_enable_tpu_release,
                                    source_mix_queue_burst_reconnect,
                                    source_mix_queue_burst_multi_reconnect,
                                    disable_enable_queue_burst_reconnect,
                                    quarantine_disable_enable_queue_inflight,
                                    quarantine_url_churn_queue_inflight,
                                    external_scenario,
                                    seq_id_max_once,
                                    seq_id_max_replay_after_reconnect,
	                                    duplicate_seq_split,
                                    duplicate_seq_split_reconnect,
                                    seq_collision_same_conn,
                                    seq_collision_reconnect,
                                    mixed_multi_batch, mixed_empty_multi_batch,
                                    mixed_malformed_multi_batch,
                                    mixed_bad_signature_multi_batch,
                                    mixed_bad_signature_reconnect,
                                    mixed_stale_multi_batch,
                                    mixed_stale_reconnect,
                                    mixed_terminal_producers_reconnect,
                                    random_mixed_multi_batch, generated_bundle,
                                    vote_reject_once,
                                    vote_reject_reconnect,
                                    raw_kunorpus_once, raw_kunorpus_reconnect,
                                    stale_slot_reject, stale_slot_reject_reconnect,
                                    empty_batch_reject, empty_batch_reject_reconnect,
                                    malformed_first_atomic,
                                    malformed_first_atomic_reconnect,
                                    malformed_tail_atomic,
                                    malformed_tail_atomic_reconnect,
                                    bad_signature_first_atomic,
                                    bad_signature_first_atomic_reconnect,
                                    bad_signature_tail_atomic,
                                    bad_signature_tail_atomic_reconnect,
                                    non_atomic_single_packet,
                                    valid_alt_commit, invalid_alt_missing_table,
                                    invalid_alt_missing_table_reconnect,
                                    bam_fee_priority_commit,
                                    bam_fee_priority_replay_after_reconnect,
                                    bam_fee_queue_burst_reconnect,
                                    bam_fee_source_mix_queue_burst_reconnect,
                                    bam_fee_url_churn_priority_commit,
                                    bam_fee_url_churn_same_slot_priority_commit,
                                    bam_fee_config_refresh_priority_commit,
                                    bam_fee_config_commission_refresh_priority_commit,
                                    bam_fee_config_refresh_queue_burst,
                                    bam_fee_config_refresh_source_mix_queue_burst,
                                    bam_fee_config_midqueue_refresh,
                                    bam_fee_config_midqueue_source_mix_queue_burst,
                                    bam_fee_config_midqueue_source_mix_multi_reconnect,
                                    fee_only_commit, fee_only_reconnect,
                                    durable_nonce_commit, durable_nonce_reconnect,
                                    durable_nonce_replay_after_reconnect,
                                    durable_nonce_wrong_authority,
                                    durable_nonce_wrong_authority_reconnect,
                                    bam_cu_limit_fail,
                                    bam_cu_limit_fail_reconnect,
                                    atomic_revert,
                                    atomic_revert_reconnect,
                                    atomic_first_overdraft,
                                    atomic_first_overdraft_reconnect,
                                    atomic_mid_fail,
                                    atomic_mid_fail_reconnect,
                                    atomic_blockhash_mid_fail,
                                    atomic_blockhash_mid_fail_reconnect,
                                    atomic_resolver_mid_fail,
                                    atomic_resolver_mid_fail_reconnect,
                                    atomic_duplicate_sig_mid_fail,
                                    atomic_duplicate_sig_mid_fail_reconnect,
                                    non_atomic_inconsistent_bundle,
                                    non_atomic_first_overdraft,
                                    non_atomic_mid_overdraft,
                                    non_atomic_partial_overdraft,
                                    non_atomic_partial_overdraft_reconnect,
                                    non_atomic_partial_resolver_fail,
                                    non_atomic_partial_blockhash_fail,
                                    non_atomic_partial_duplicate_sig,
                                    non_atomic_partial_cu_fail,
                                    disable_enable_unique_after_reconnect,
                                    url_churn_unique_after_reconnect,
                                    fd_pause_resume_churn,
                                    bam_pause_resume_churn,
                                    fd_restart_churn, bam_restart_churn,
                                    url_sni_churn
  --mode-list CSV                   Deterministic comma-separated mode list;
                                    iteration N uses entry N modulo length
  --scenario-file PATH              Replay an existing scripted BAM scenario;
                                    implies --mode external_scenario when
                                    --mode is left at random
  --operator-events-file PATH       Run an existing operator event sequence
                                    alongside the selected live scenario
  --input-family NAME               One of: synthetic, external_scenario,
                                    kunorpus_system, kunorpus_vote,
                                    kunorpus_raw_txn, kunorpus_bundle, random
                                    (default: synthetic)
  --kunorpus-count N                kunorpus generate instr count when
                                    using a kunorpus-backed family (default: 64)
  --kunorpus-seed-window N          Number of consecutive kunorpus seeds to try
                                    when selecting an executable input
                                    (default: 16)
  --kunorpus-max-transfer-lamports N
                                    Upper bound when selecting a local executable
                                    transfer from the kunorpus corpus
  --kunorpus-system-kind NAME      For kunorpus_system, require one of: any,
                                    transfer, assign, allocate, create_account,
                                    transfer_with_seed, create_account_with_seed
                                    (default: any)
  --generated-bundle-max-batches N Maximum batches in one generated scheduler
                                    message (default: 8, maximum: 128)
  --generated-bundle-max-packets N Maximum independently generated packets per
                                    batch (default: 5, protocol maximum: 5)
  --timeout-secs N                  Per-iteration timeout (default: 90)
  --allow-operator-perturbations    Include low-frequency operator churn in random mode
  --allow-self-check-failures       Preserve invalid normalized outcomes and continue;
                                    intended for differential campaigns with known failures
  --live-coverage-dir PATH          Write LLVM profiles from the live validator under PATH
  --queue-burst-batch-count N       Override queue-burst helper batch count
  --queue-burst-max-schedule-slot VALUE
                                    Override queue-burst max_schedule_slot
                                    (default depends on mode; supports max, leader, or leader+N)
  --queue-burst-close-after-results N
                                    Override queue-burst reconnect point
  --normal-tpu-burst-count N        Send N copies through normal TPU while BAM is disabled
                                    (default: 1, maximum: 128)
  --normal-tpu-matrix-count N       Generate and send N distinct System-program packets
                                    while BAM is disabled (default: 0, maximum: 64)
  --bam-disabled-hold-ms N          Keep BAM disabled for N milliseconds
                                    (default: 6000)
  --bam-disabled-workload-script PATH
                                    Run an executable coverage workload while BAM is disabled
  --from-genesis-account N          Genesis-funded payer index (default: 0)
  --to-genesis-account-one N        First recipient genesis index (default: 1)
  --to-genesis-account-two N        Second recipient genesis index (default: 2)
  -h, --help                        Show this help
EOF2
}

die() {
  echo "error: $*" >&2
  exit 1
}

run_logged() {
  local label="$1"
  local log_file="$2"
  shift 2

  if "$@" >>"${log_file}" 2>&1; then
    return 0
  fi

  echo "error: ${label} failed; tail of ${log_file}:" >&2
  tail -n 200 "${log_file}" >&2 || true
  return 1
}

run_logged_retry() {
  local label="$1"
  local log_file="$2"
  local attempts="$3"
  local delay_secs="$4"
  shift 4

  local attempt
  for ((attempt=1; attempt<=attempts; attempt++)); do
    if "$@" >>"${log_file}" 2>&1; then
      return 0
    fi
    if (( attempt < attempts )); then
      printf 'retrying %s after attempt %d/%d\n' "${label}" "${attempt}" "${attempts}" >>"${log_file}"
      sleep "${delay_secs}"
    fi
  done

  echo "error: ${label} failed after ${attempts} attempts; tail of ${log_file}:" >&2
  tail -n 200 "${log_file}" >&2 || true
  return 1
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
    --target-name)
      TARGET_NAME="$2"
      shift 2
      ;;
    --fddev-bin)
      FDDEV_BIN="$2"
      shift 2
      ;;
    --runner-kind)
      RUNNER_KIND="$2"
      shift 2
      ;;
    --solana-bin-dir)
      SOLANA_BIN_DIR="$2"
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
    --check-bam-shred)
      CHECK_BAM_SHRED=1
      shift
      ;;
    --log-dir)
      LOG_DIR="$2"
      shift 2
      ;;
    --iterations)
      ITERATIONS="$2"
      shift 2
      ;;
    --seed)
      SEED="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --mode-list)
      MODE_LIST="$2"
      shift 2
      ;;
    --scenario-file)
      EXTERNAL_SCENARIO_FILE="$2"
      shift 2
      ;;
    --operator-events-file)
      EXTERNAL_OPERATOR_EVENTS_FILE="$2"
      shift 2
      ;;
    --input-family)
      INPUT_FAMILY="$2"
      shift 2
      ;;
    --kunorpus-count)
      KUNORPUS_COUNT="$2"
      shift 2
      ;;
    --kunorpus-seed-window)
      KUNORPUS_SEED_WINDOW="$2"
      shift 2
      ;;
    --kunorpus-max-transfer-lamports)
      KUNORPUS_MAX_TRANSFER_LAMPORTS="$2"
      shift 2
      ;;
    --kunorpus-system-kind)
      KUNORPUS_SYSTEM_KIND="$2"
      shift 2
      ;;
    --generated-bundle-max-batches)
      GENERATED_BUNDLE_MAX_BATCHES="$2"
      shift 2
      ;;
    --generated-bundle-max-packets)
      GENERATED_BUNDLE_MAX_PACKETS="$2"
      shift 2
      ;;
    --timeout-secs)
      TIMEOUT_SECS="$2"
      shift 2
      ;;
    --allow-operator-perturbations)
      ALLOW_OPERATOR_PERTURBATIONS=1
      shift
      ;;
    --allow-self-check-failures)
      ALLOW_SELF_CHECK_FAILURES=1
      shift
      ;;
    --live-coverage-dir)
      LIVE_COVERAGE_DIR="$2"
      shift 2
      ;;
    --queue-burst-batch-count)
      QUEUE_BURST_BATCH_COUNT="$2"
      shift 2
      ;;
    --queue-burst-max-schedule-slot)
      QUEUE_BURST_MAX_SCHEDULE_SLOT="$2"
      shift 2
      ;;
    --queue-burst-close-after-results)
      QUEUE_BURST_CLOSE_AFTER_RESULTS="$2"
      shift 2
      ;;
    --normal-tpu-burst-count)
      NORMAL_TPU_BURST_COUNT="$2"
      shift 2
      ;;
    --normal-tpu-matrix-count)
      NORMAL_TPU_MATRIX_COUNT="$2"
      shift 2
      ;;
    --bam-disabled-hold-ms)
      BAM_DISABLED_HOLD_MS="$2"
      shift 2
      ;;
    --bam-disabled-workload-script)
      BAM_DISABLED_WORKLOAD_SCRIPT="$2"
      shift 2
      ;;
    --from-genesis-account)
      FROM_GENESIS_ACCOUNT="$2"
      shift 2
      ;;
    --to-genesis-account-one)
      TO_GENESIS_ACCOUNT_ONE="$2"
      shift 2
      ;;
    --to-genesis-account-two)
      TO_GENESIS_ACCOUNT_TWO="$2"
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

regex_escape() {
  printf '%s' "$1" | sed -e 's/[][(){}.^$*+?|\\/]/\\&/g'
}

BAM_URL_RE=$(regex_escape "${BAM_URL}")
BAM_BAD_URL_RE=$(regex_escape "${BAM_BAD_URL}")

FDDEV="${FDDEV_BIN:-${FDDEV:-${ROOT}/build/native/gcc/bin/fddev}}"
BRIDGE_MANIFEST="${ROOT}/contrib/txnctx-bridge/Cargo.toml"
BRIDGE_BUILD_DIR="${ROOT}/contrib/txnctx-bridge/target/debug"
BRIDGE_BIN="${BRIDGE_BUILD_DIR}/txnctx-bridge"
GEN_SIMPLE_SYSTEM_TXNCTX_BIN="${BRIDGE_BUILD_DIR}/gen_simple_system_txnctx"
GEN_PROGRAM_INVOCATION_BIN="${BRIDGE_BUILD_DIR}/gen_program_invocation"
GEN_DURABLE_NONCE_TRANSFER_BIN="${BRIDGE_BUILD_DIR}/gen_durable_nonce_transfer"
WRITE_SEEDED_KEYPAIR_BIN="${BRIDGE_BUILD_DIR}/write_seeded_keypair"
MAKE_ALT_SETUP_TXNS_BIN="${BRIDGE_BUILD_DIR}/make_alt_setup_txns"
DECODE_SLOT_HASHES_BIN="${BRIDGE_BUILD_DIR}/decode_slot_hashes_account"
BAM_PRESEEDED_ALT_TABLE_SEED="${BAM_PRESEEDED_ALT_TABLE_SEED:-424242}"
BAM_PRESEEDED_NONCE_HASH_SEED_BASE="${BAM_PRESEEDED_NONCE_HASH_SEED_BASE:-1900000}"
BAM_PRESEEDED_PROGRAM_ELF="${BAM_PRESEEDED_PROGRAM_ELF:-}"
BAM_PRESEEDED_PROGRAM_ID="${BAM_PRESEEDED_PROGRAM_ID:-}"
FULLFD_SNAPSHOT_FILE="${FULLFD_SNAPSHOT_FILE:-}"
FULLFD_SNAPSHOT_DIR="${FULLFD_SNAPSHOT_DIR:-}"
FULLFD_SNAPSHOT_BOOTSTRAP_CONFIG="${FULLFD_SNAPSHOT_BOOTSTRAP_CONFIG:-}"
FULLFD_SNAPSHOT_GENESIS_FILE="${FULLFD_SNAPSHOT_GENESIS_FILE:-}"
FULLFD_SNAPSHOT_GENESIS_DEST="${FULLFD_SNAPSHOT_GENESIS_DEST:-}"
BAM_VALID_ALT_MIN_FINALIZED_SLOT="${BAM_VALID_ALT_MIN_FINALIZED_SLOT:-1}"
BAM_MANIFEST="${ROOT}/contrib/bam-test-server/Cargo.toml"
NORMALIZE_OUTCOME="${ROOT}/contrib/normalize-bam-outcome.py"
CHECK_OUTCOME="${ROOT}/contrib/check-bam-outcome.py"
BAM_SHRED_CAPTURE_BIN="${ROOT}/contrib/capture-udp-packets.py"
CAPTURE_CHAIN_EVIDENCE="${ROOT}/contrib/capture-bam-chain-evidence.py"
MATERIALIZE_SCENARIO="${ROOT}/contrib/materialize-bam-scenario-packets.py"
OPERATOR_RUNNER="${ROOT}/contrib/run-bam-operator-events.sh"
PARTIAL_DRAIN_RUNNER="${ROOT}/contrib/validate-local-bam-partial-drain-reconnect.sh"
DUPLICATE_SEQ_SPLIT_RUNNER="${ROOT}/contrib/validate-local-bam-duplicate-seq-split.sh"
FD_RESTART_RUNNER="${ROOT}/contrib/validate-local-bam-fd-restart.sh"
BAM_RESTART_RUNNER="${ROOT}/contrib/validate-local-bam-controller-restart.sh"
ATOMIC_REVERT_RUNNER="${ROOT}/contrib/validate-local-bam-atomic-revert.sh"
FD_PAUSE_RESUME_RUNNER="${ROOT}/contrib/validate-local-bam-fd-pause-resume.sh"
BAM_PAUSE_RESUME_RUNNER="${ROOT}/contrib/validate-local-bam-controller-pause-resume.sh"
URL_SNI_CHURN_RUNNER="${ROOT}/contrib/validate-local-bam-url-sni-churn.sh"
KUNORPUS="${KUNORPUS:-${HOME}/solfuzz/kunorpus/target/release/kunorpus}"

[[ -x "${FDDEV}" ]] || die "missing validator binary ${FDDEV}"
if [[ ! -x "${FDCTL_BIN}" ]]; then
  FDCTL_BIN="${FDDEV}"
fi
[[ -f "${BRIDGE_MANIFEST}" ]] || die "missing ${BRIDGE_MANIFEST}"
[[ -f "${BAM_MANIFEST}" ]] || die "missing ${BAM_MANIFEST}"
[[ -x "${NORMALIZE_OUTCOME}" ]] || die "missing executable ${NORMALIZE_OUTCOME}"
[[ -x "${CHECK_OUTCOME}" ]] || die "missing executable ${CHECK_OUTCOME}"
[[ -f "${BAM_SHRED_CAPTURE_BIN}" ]] || die "missing ${BAM_SHRED_CAPTURE_BIN}"
[[ -x "${CAPTURE_CHAIN_EVIDENCE}" ]] || die "missing executable ${CAPTURE_CHAIN_EVIDENCE}"
[[ -x "${MATERIALIZE_SCENARIO}" ]] || die "missing executable ${MATERIALIZE_SCENARIO}"
[[ -x "${OPERATOR_RUNNER}" ]] || die "missing ${OPERATOR_RUNNER}"
[[ -x "${PARTIAL_DRAIN_RUNNER}" ]] || die "missing ${PARTIAL_DRAIN_RUNNER}"
[[ -x "${DUPLICATE_SEQ_SPLIT_RUNNER}" ]] || die "missing ${DUPLICATE_SEQ_SPLIT_RUNNER}"
[[ -x "${FD_RESTART_RUNNER}" ]] || die "missing ${FD_RESTART_RUNNER}"
[[ -x "${BAM_RESTART_RUNNER}" ]] || die "missing ${BAM_RESTART_RUNNER}"
[[ -x "${URL_SNI_CHURN_RUNNER}" ]] || die "missing ${URL_SNI_CHURN_RUNNER}"
[[ -x "${ATOMIC_REVERT_RUNNER}" ]] || die "missing ${ATOMIC_REVERT_RUNNER}"
[[ -x "${FD_PAUSE_RESUME_RUNNER}" ]] || die "missing ${FD_PAUSE_RESUME_RUNNER}"
[[ -x "${BAM_PAUSE_RESUME_RUNNER}" ]] || die "missing ${BAM_PAUSE_RESUME_RUNNER}"
[[ -f "${CONFIG}" ]] || die "missing config ${CONFIG}"
command -v cargo >/dev/null 2>&1 || die "cargo not found"
command -v curl >/dev/null 2>&1 || die "curl not found"
command -v jq >/dev/null 2>&1 || die "jq not found"

if [[ -n "${LIVE_COVERAGE_DIR}" ]]; then
  command -v gdb >/dev/null 2>&1 || die "gdb is required for live validator coverage"
  mkdir -p "${LIVE_COVERAGE_DIR}"
  chmod a+rwx "${LIVE_COVERAGE_DIR}"
fi

if [[ -n "${SOLANA_BIN_DIR}" ]]; then
  [[ -d "${SOLANA_BIN_DIR}" ]] || die "missing --solana-bin-dir ${SOLANA_BIN_DIR}"
  export PATH="${SOLANA_BIN_DIR}:${PATH}"
fi

if [[ -n "${EXTERNAL_SCENARIO_FILE}" ]]; then
  [[ -f "${EXTERNAL_SCENARIO_FILE}" ]] || die "missing scenario file ${EXTERNAL_SCENARIO_FILE}"
  if [[ "${MODE}" == "random" ]]; then
    MODE="external_scenario"
  fi
fi
if [[ -n "${EXTERNAL_OPERATOR_EVENTS_FILE}" ]]; then
  [[ -f "${EXTERNAL_OPERATOR_EVENTS_FILE}" ]] || die "missing operator events file ${EXTERNAL_OPERATOR_EVENTS_FILE}"
fi
if [[ "${MODE}" == "external_scenario" && -z "${EXTERNAL_SCENARIO_FILE}" ]]; then
  die "--mode external_scenario requires --scenario-file"
fi
if [[ "${MODE}" == "durable_nonce_commit" || "${MODE}" == "durable_nonce_reconnect" || "${MODE}" == "durable_nonce_replay_after_reconnect" || "${MODE}" == "durable_nonce_wrong_authority" || "${MODE}" == "durable_nonce_wrong_authority_reconnect" ]]; then
  command -v solana >/dev/null 2>&1 || die "solana not found"
fi

cargo build --quiet --manifest-path "${BRIDGE_MANIFEST}" --bins >/dev/null
[[ -x "${BRIDGE_BIN}" ]] || die "missing ${BRIDGE_BIN}; bridge build failed"
[[ -x "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" ]] || die "missing ${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}; bridge build failed"
[[ -x "${GEN_PROGRAM_INVOCATION_BIN}" ]] || die "missing ${GEN_PROGRAM_INVOCATION_BIN}; bridge build failed"
[[ -x "${GEN_DURABLE_NONCE_TRANSFER_BIN}" ]] || die "missing ${GEN_DURABLE_NONCE_TRANSFER_BIN}; bridge build failed"
[[ -x "${DECODE_SLOT_HASHES_BIN}" ]] || die "missing ${DECODE_SLOT_HASHES_BIN}; bridge build failed"
if [[ "${MODE}" == "valid_alt_commit" || "${MODE}" == "durable_nonce_commit" || "${MODE}" == "durable_nonce_reconnect" || "${MODE}" == "durable_nonce_replay_after_reconnect" || "${MODE}" == "durable_nonce_wrong_authority" || "${MODE}" == "durable_nonce_wrong_authority_reconnect" ]]; then
  [[ -x "${WRITE_SEEDED_KEYPAIR_BIN}" ]] || die "missing ${WRITE_SEEDED_KEYPAIR_BIN}; bridge build failed"
fi
if [[ "${RUNNER_KIND}" == "fullfd" ]]; then
  cargo build --quiet --manifest-path "${BAM_MANIFEST}" --bin bam-test-server >/dev/null
fi

(( ITERATIONS >= 1 )) || die "--iterations must be at least 1"
[[ "${GENERATED_BUNDLE_MAX_BATCHES}" =~ ^[0-9]+$ ]] \
  && (( GENERATED_BUNDLE_MAX_BATCHES >= 1 && GENERATED_BUNDLE_MAX_BATCHES <= 128 )) \
  || die "--generated-bundle-max-batches must be an integer from 1 through 128"
[[ "${GENERATED_BUNDLE_MAX_PACKETS}" =~ ^[0-9]+$ ]] \
  && (( GENERATED_BUNDLE_MAX_PACKETS >= 0 && GENERATED_BUNDLE_MAX_PACKETS <= 5 )) \
  || die "--generated-bundle-max-packets must be an integer from 0 through 5"
(( FROM_GENESIS_ACCOUNT != TO_GENESIS_ACCOUNT_ONE )) || die "payer and first recipient must differ"
(( FROM_GENESIS_ACCOUNT != TO_GENESIS_ACCOUNT_TWO )) || die "payer and second recipient must differ"
(( TO_GENESIS_ACCOUNT_ONE != TO_GENESIS_ACCOUNT_TWO )) || die "recipient indices must differ"

case "${MODE}" in
random|commit_once|replay_same_conn|replay_after_reconnect|unique_after_reconnect|seq_id_wrap_sequence|seq_id_out_of_order_multi_batch|seq_id_wrap_out_of_order_multi_batch|seq_id_wrap_conflicting_spend_multi_batch|partial_drain_reconnect|queue_burst_reconnect|queue_burst64_reconnect|queue_burst64_leader_plus1_reconnect|schedule_boundary_jitter|queue_reconnect_timing_jitter|queue_burst_multi_reconnect|queue_burst128_reconnect|queue_burst256_reconnect|queue_burst512_reconnect|queue_burst_leader_reconnect|queue_burst64_leader_reconnect|bam_fee_queue_burst_reconnect|bam_fee_source_mix_queue_burst_reconnect|bam_fee_config_refresh_queue_burst|bam_fee_config_refresh_source_mix_queue_burst|bam_fee_config_midqueue_refresh|bam_fee_config_midqueue_source_mix_queue_burst|bam_fee_config_midqueue_source_mix_multi_reconnect|source_mix_bam_tpu|source_mix_precommit|source_mix_atomic_revert_precommit|source_mix_duplicate_tpu_after_bam|disable_enable_tpu_release|source_mix_queue_burst_reconnect|source_mix_queue_burst_multi_reconnect|disable_enable_queue_burst_reconnect|quarantine_disable_enable_queue_inflight|quarantine_url_churn_queue_inflight|external_scenario|seq_id_max_once|seq_id_max_replay_after_reconnect|duplicate_seq_split|duplicate_seq_split_reconnect|seq_collision_same_conn|seq_collision_reconnect|mixed_multi_batch|mixed_empty_multi_batch|mixed_malformed_multi_batch|mixed_bad_signature_multi_batch|mixed_bad_signature_reconnect|mixed_stale_multi_batch|mixed_stale_reconnect|mixed_terminal_producers_reconnect|random_mixed_multi_batch|generated_bundle|vote_reject_once|vote_reject_reconnect|raw_kunorpus_once|raw_kunorpus_reconnect|stale_slot_reject|stale_slot_reject_reconnect|empty_batch_reject|empty_batch_reject_reconnect|malformed_first_atomic|malformed_first_atomic_reconnect|malformed_tail_atomic|malformed_tail_atomic_reconnect|bad_signature_first_atomic|bad_signature_first_atomic_reconnect|bad_signature_tail_atomic|bad_signature_tail_atomic_reconnect|non_atomic_single_packet|valid_alt_commit|invalid_alt_missing_table|invalid_alt_missing_table_reconnect|bam_fee_priority_commit|bam_fee_priority_replay_after_reconnect|bam_fee_url_churn_priority_commit|bam_fee_url_churn_same_slot_priority_commit|bam_fee_config_refresh_priority_commit|bam_fee_config_commission_refresh_priority_commit|fee_only_commit|fee_only_reconnect|durable_nonce_commit|durable_nonce_reconnect|durable_nonce_replay_after_reconnect|durable_nonce_wrong_authority|durable_nonce_wrong_authority_reconnect|bam_cu_limit_fail|bam_cu_limit_fail_reconnect|atomic_revert|atomic_revert_reconnect|atomic_first_overdraft|atomic_first_overdraft_reconnect|atomic_mid_fail|atomic_mid_fail_reconnect|atomic_blockhash_mid_fail|atomic_blockhash_mid_fail_reconnect|atomic_resolver_mid_fail|atomic_resolver_mid_fail_reconnect|atomic_duplicate_sig_mid_fail|atomic_duplicate_sig_mid_fail_reconnect|non_atomic_inconsistent_bundle|non_atomic_first_overdraft|non_atomic_mid_overdraft|non_atomic_partial_overdraft|non_atomic_partial_overdraft_reconnect|non_atomic_partial_resolver_fail|non_atomic_partial_blockhash_fail|non_atomic_partial_duplicate_sig|non_atomic_partial_cu_fail|non_atomic_valid_multi_packet|disable_enable_unique_after_reconnect|url_churn_unique_after_reconnect|fd_pause_resume_churn|bam_pause_resume_churn|fd_restart_churn|bam_restart_churn|url_sni_churn)
    ;;
  *)
    die "unsupported --mode ${MODE}"
    ;;
esac

case "${INPUT_FAMILY}" in
  synthetic|external_scenario|kunorpus_system|kunorpus_vote|kunorpus_raw_txn|kunorpus_bundle|random)
    ;;
  *)
    die "unsupported --input-family ${INPUT_FAMILY}"
    ;;
esac

if [[ -n "${QUEUE_BURST_BATCH_COUNT}" ]]; then
  [[ "${QUEUE_BURST_BATCH_COUNT}" =~ ^[0-9]+$ && "${QUEUE_BURST_BATCH_COUNT}" -gt 1 ]] \
    || die "--queue-burst-batch-count must be an integer greater than 1"
fi

[[ "${NORMAL_TPU_BURST_COUNT}" =~ ^[0-9]+$ ]] \
  && (( NORMAL_TPU_BURST_COUNT >= 1 && NORMAL_TPU_BURST_COUNT <= 128 )) \
  || die "--normal-tpu-burst-count must be an integer from 1 through 128"
[[ "${NORMAL_TPU_MATRIX_COUNT}" =~ ^[0-9]+$ ]] \
  && (( NORMAL_TPU_MATRIX_COUNT >= 0 && NORMAL_TPU_MATRIX_COUNT <= 64 )) \
  || die "--normal-tpu-matrix-count must be an integer from 0 through 64"
[[ "${BAM_DISABLED_HOLD_MS}" =~ ^[0-9]+$ ]] && (( BAM_DISABLED_HOLD_MS >= 1000 )) \
  || die "--bam-disabled-hold-ms must be an integer of at least 1000"
if [[ -n "${BAM_DISABLED_WORKLOAD_SCRIPT}" ]]; then
  [[ -x "${BAM_DISABLED_WORKLOAD_SCRIPT}" ]] \
    || die "missing executable BAM-disabled workload ${BAM_DISABLED_WORKLOAD_SCRIPT}"
fi

case "${INPUT_FAMILY}" in
  kunorpus_system|kunorpus_vote|kunorpus_raw_txn|kunorpus_bundle|random)
    [[ -x "${KUNORPUS}" ]] || die "missing ${KUNORPUS}; build kunorpus first"
    ;;
esac

if [[ "${INPUT_FAMILY}" == "kunorpus_vote" && "${MODE}" != "random" && "${MODE}" != "vote_reject_once" && "${MODE}" != "vote_reject_reconnect" ]]; then
  die "--input-family kunorpus_vote requires --mode vote_reject_once, --mode vote_reject_reconnect, or --mode random"
fi

if [[ "${INPUT_FAMILY}" == "kunorpus_raw_txn" && "${MODE}" != "random" && "${MODE}" != "raw_kunorpus_once" && "${MODE}" != "raw_kunorpus_reconnect" ]]; then
  die "--input-family kunorpus_raw_txn requires --mode raw_kunorpus_once, --mode raw_kunorpus_reconnect, or --mode random"
fi

if [[ "${INPUT_FAMILY}" == "kunorpus_bundle" && "${MODE}" != "random" && "${MODE}" != "generated_bundle" ]]; then
  die "--input-family kunorpus_bundle requires --mode generated_bundle or --mode random"
fi

case "${KUNORPUS_SYSTEM_KIND}" in
  any|transfer|assign|allocate|create_account|transfer_with_seed|create_account_with_seed)
    ;;
  *)
    die "unsupported --kunorpus-system-kind ${KUNORPUS_SYSTEM_KIND}"
    ;;
esac

SUDO=()
if [[ "${USE_SUDO}" != "0" && "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo not found"
  SUDO=( sudo )
fi

if [[ -z "${LOG_DIR}" ]]; then
  LOG_DIR=$(mktemp -d /tmp/firebam-live-stateful-fuzz.XXXXXX)
fi
mkdir -p "${LOG_DIR}"

FD_PID=""
FD_SUPERVISOR_PID=""
BAM_PID=""
OPERATOR_PID=""
BAM_SHRED_CAPTURE_PID=""
CONFIG_INITIALIZED=0
EXTRA_BAM_PIDS=()
CURRENT_FD_PID_FILE=""
CURRENT_BAM_PID_FILE=""
CURRENT_BAM_START_SCRIPT=""
CURRENT_BAM_BUILDER_PUBKEY=""
CURRENT_BAM_OMIT_BLOCK_ENGINE_CONFIG=0
ITER_SYSTEM_KIND=""
ITER_OWNER_ONE=""
ITER_OWNER_TWO=""
ITER_SPACE_ONE=""
ITER_SPACE_TWO=""
ITER_OWNER_ONE_OBSERVED=""
ITER_OWNER_TWO_OBSERVED=""
ITER_SPACE_ONE_OBSERVED=""
ITER_SPACE_TWO_OBSERVED=""
NORMAL_TPU_MATRIX_DIR=""
ITER_SIMPLE_VOTE_NODE=""
ITER_VOTE_ACCOUNT=""
ITER_AUTHORIZED_VOTER=""
ITER_VOTE_SLOT=""
ITER_VOTE_HASH=""
ITER_PRE_AUTHORIZED_VOTER=""
ITER_PRE_VOTES_LEN=""
ITER_PRE_LAST_TIMESTAMP_SLOT=""
ITER_VOTE_ACCOUNT_JSON_UNSUPPORTED=0
ITER_OBSERVED_TO_ONE=""
ITER_OBSERVED_TO_TWO=""
ITER_POST_AUTHORIZED_VOTER=""
ITER_POST_VOTES_LEN=""
ITER_POST_LAST_TIMESTAMP_SLOT=""
ITER_BAM_FEE_RECIPIENT=""
ITER_BAM_FEE_RECIPIENT_INITIAL=""
ITER_BAM_FEE_RECIPIENT_OBSERVED=""
ITER_BAM_FEE_RECIPIENT_SECOND=""
ITER_BAM_FEE_RECIPIENT_SECOND_INITIAL=""
ITER_BAM_FEE_RECIPIENT_SECOND_OBSERVED=""
ITER_BAM_FEE_CHURN_TRANSFER_ONE=""
ITER_BAM_FEE_CHURN_TRANSFER_TWO=""
ITER_BAM_SECONDARY_LOG=""

dump_live_process_coverage() {
  local pid="$1"
  local tile="$2"
  local profile escaped_profile gdb_log

  profile="${LIVE_COVERAGE_DIR}/${TARGET_NAME}-${ITER_MODE}-${tile}-${pid}.profraw"
  escaped_profile="${profile//\\/\\\\}"
  escaped_profile="${escaped_profile//\"/\\\"}"
  gdb_log="${profile%.profraw}.gdb.log"
  "${SUDO[@]}" gdb -q -nx -batch -p "${pid}" \
    -ex "call (void)__llvm_profile_set_filename(\"${escaped_profile}\")" \
    -ex 'call (int)__llvm_profile_write_file()' \
    -ex detach >"${gdb_log}" 2>&1
  if [[ ! -s "${profile}" ]]; then
    echo "warning: failed to collect live coverage from ${tile} tile pid ${pid}; see ${gdb_log}" >&2
  fi
}

dump_live_tile_coverage() {
  [[ -n "${LIVE_COVERAGE_DIR}" && -n "${FD_PID}" ]] || return 0

  if [[ "${RUNNER_KIND}" != "fullfd" ]]; then
    dump_live_process_coverage "${FD_PID}" hybrid
  fi

  local pid args tile kind_id
  while IFS=' ' read -r pid args; do
    [[ -n "${pid}" && -n "${args}" ]] || continue
    if [[ "${RUNNER_KIND}" != "fullfd" && "${args}" =~ [[:space:]]run-agave([[:space:]]|$) ]]; then
      dump_live_process_coverage "${pid}" agave
    elif [[ "${args}" =~ [[:space:]]run1[[:space:]]+([^[:space:]]+)[[:space:]]+([0-9]+)([[:space:]]|$) ]]; then
      tile="${BASH_REMATCH[1]}"
      kind_id="${BASH_REMATCH[2]}"
      dump_live_process_coverage "${pid}" "${tile}-${kind_id}"
    fi
  done < <(ps -o pid=,args= -g "${FD_PID}")
}

cleanup_iteration() {
  local restore_errexit=0
  [[ $- == *e* ]] && restore_errexit=1
  set +e
  if [[ -n "${OPERATOR_PID}" ]]; then
    kill "${OPERATOR_PID}" 2>/dev/null || true
    wait "${OPERATOR_PID}" 2>/dev/null || true
    OPERATOR_PID=""
  fi
  if [[ -n "${BAM_SHRED_CAPTURE_PID}" ]]; then
    kill "${BAM_SHRED_CAPTURE_PID}" 2>/dev/null || true
    wait "${BAM_SHRED_CAPTURE_PID}" 2>/dev/null || true
    BAM_SHRED_CAPTURE_PID=""
  fi
  if [[ -n "${FD_PID}" ]]; then
    dump_live_tile_coverage
    "${SUDO[@]}" kill -- "-${FD_PID}" 2>/dev/null || true
    FD_PID=""
  fi
  if [[ -n "${FD_SUPERVISOR_PID}" ]]; then
    kill "${FD_SUPERVISOR_PID}" 2>/dev/null || true
    wait "${FD_SUPERVISOR_PID}" 2>/dev/null || true
    FD_SUPERVISOR_PID=""
  fi
  if [[ -n "${BAM_PID}" ]]; then
    kill "${BAM_PID}" 2>/dev/null || true
    wait "${BAM_PID}" 2>/dev/null || true
    BAM_PID=""
  fi
  local extra_pid
  for extra_pid in "${EXTRA_BAM_PIDS[@]}"; do
    kill "${extra_pid}" 2>/dev/null || true
    wait "${extra_pid}" 2>/dev/null || true
  done
  EXTRA_BAM_PIDS=()
  if [[ "${restore_errexit}" == "1" ]]; then
    set -e
  fi
}

cleanup_all() {
  trap - EXIT INT TERM
  cleanup_iteration
  if [[ "${CONFIG_INITIALIZED}" == "1" ]]; then
    local fini_env=( env )
    if [[ -n "${LIVE_COVERAGE_DIR}" ]]; then
      fini_env+=( "LLVM_PROFILE_FILE=${LIVE_COVERAGE_DIR}/${TARGET_NAME}-${ITER_MODE:-cleanup}-configure-fini-%m-%p.profraw" )
    fi
    "${SUDO[@]}" "${fini_env[@]}" "${FDDEV}" configure fini all --config "${CONFIG}" >/dev/null 2>&1 || true
    CONFIG_INITIALIZED=0
  fi
}

trap cleanup_all EXIT INT TERM

rpc_latest_blockhash() {
  curl "${RPC_CURL_ARGS[@]}" "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash","params":[{"commitment":"processed"}]}' \
    | jq -e -r '.result.value.blockhash'
}

rpc_health() {
  curl "${RPC_CURL_ARGS[@]}" "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}'
}

rpc_slot() {
  curl "${RPC_CURL_ARGS[@]}" "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"processed"}]}' \
    | jq -e -r '.result'
}

rpc_finalized_slot() {
  curl "${RPC_CURL_ARGS[@]}" "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"finalized"}]}' \
    | jq -e -r '.result'
}

rpc_block_signatures() {
  local slot="$1"
  curl "${RPC_CURL_ARGS[@]}" "${RPC_URL}" \
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
  curl "${RPC_CURL_ARGS[@]}" "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBalance\",\"params\":[\"${pubkey}\",{\"commitment\":\"processed\"}]}" \
    | jq -e -r '.result.value'
}

rpc_account_base64() {
  local pubkey="$1"
  curl "${RPC_CURL_ARGS[@]}" "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAccountInfo\",\"params\":[\"${pubkey}\",{\"encoding\":\"base64\",\"commitment\":\"processed\"}]}" \
    | jq -e -r '.result.value.data[0]'
}

rpc_vote_account_json() {
  local pubkey="$1"
  curl "${RPC_CURL_ARGS[@]}" "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAccountInfo\",\"params\":[\"${pubkey}\",{\"encoding\":\"jsonParsed\",\"commitment\":\"processed\"}]}"
}

rpc_account_info() {
  local pubkey="$1"
  curl "${RPC_CURL_ARGS[@]}" "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAccountInfo\",\"params\":[\"${pubkey}\",{\"encoding\":\"base64\",\"commitment\":\"processed\"}]}"
}

rpc_account_owner() {
  local pubkey="$1"
  rpc_account_info "${pubkey}" | jq -r '.result.value.owner // empty'
}

rpc_account_space() {
  local pubkey="$1"
  rpc_account_info "${pubkey}" | jq -r '.result.value.space // empty'
}

rpc_send_base64_transaction() {
  local encoded="$1"
  curl "${RPC_CURL_ARGS[@]}" "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"sendTransaction\",\"params\":[\"${encoded}\",{\"encoding\":\"base64\",\"skipPreflight\":true,\"maxRetries\":20}]}" \
    | jq -e -r '.result'
}

send_fddev_base64_transaction() {
  local encoded="$1"
  local log_path="$2"

  {
    printf 'setup_tpu_dst=127.0.0.1:%s\n' "${BAM_TPU_PORT}"
    printf 'setup_payload_base64_len=%s\n' "${#encoded}"
    printf '\n'
  } >"${log_path}"

  if ! run_fddev_txn_covered \
      --config "${CONFIG}" \
      --count 1 \
      --no-ready \
      --dst-ip 127.0.0.1 \
      --dst-port "${BAM_TPU_PORT}" \
      --payload-base64-encoded "${encoded}" \
      >>"${log_path}" 2>&1; then
    echo "--- tail ${log_path} ---" >&2
    tail -n 80 "${log_path}" >&2 || true
    return 1
  fi
}

wait_fddev_tiles_ready() {
  local log_path="$1"
  if ! "${SUDO[@]}" "${FDDEV}" ready \
      --config "${CONFIG}" \
      >"${log_path}" 2>&1; then
    echo "--- tail ${log_path} ---" >&2
    tail -n 80 "${log_path}" >&2 || true
    return 1
  fi
}

wait_for_account_present() {
  local pubkey="$1"
  local timeout_secs="$2"
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    if rpc_account_info "${pubkey}" 2>/dev/null | jq -e '.result.value != null' >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

rpc_min_balance_for_rent_exemption() {
  local space="$1"
  curl "${RPC_CURL_ARGS[@]}" "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getMinimumBalanceForRentExemption\",\"params\":[${space}]}" \
    | jq -e -r '.result'
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

wait_for_pattern() {
  local file="$1"
  local pattern="$2"
  local timeout_secs="$3"
  local deadline=$((SECONDS + timeout_secs))
  while :; do
    if grep -qE "${pattern}" "${file}" 2>/dev/null; then
      return 0
    fi
    if [[ -n "${BAM_PID}" ]] && ! kill -0 "${BAM_PID}" 2>/dev/null; then
      break
    fi
    if [[ -n "${FD_PID}" ]] && ! "${SUDO[@]}" kill -0 -- "-${FD_PID}" 2>/dev/null; then
      break
    fi
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 1
  done
  sleep 1
  grep -qE "${pattern}" "${file}" 2>/dev/null
}

wait_for_pattern_count() {
  local file="$1"
  local pattern="$2"
  local expected_count="$3"
  local timeout_secs="$4"
  local deadline=$((SECONDS + timeout_secs))
  local observed=0
  while :; do
    observed=$(grep -cE "${pattern}" "${file}" 2>/dev/null || true)
    if (( observed >= expected_count )); then
      return 0
    fi
    if [[ -n "${BAM_PID}" ]] && ! kill -0 "${BAM_PID}" 2>/dev/null; then
      break
    fi
    if [[ -n "${FD_PID}" ]] && ! "${SUDO[@]}" kill -0 -- "-${FD_PID}" 2>/dev/null; then
      break
    fi
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 1
  done
  sleep 1
  observed=$(grep -cE "${pattern}" "${file}" 2>/dev/null || true)
  (( observed >= expected_count ))
}

scenario_has_send_events() {
  local scenario_file="$1"
  grep -Eq 'type[[:space:]]*=[[:space:]]*"send_(batch|batch_flood|multi_batch|split_batch)"' "${scenario_file}" 2>/dev/null
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
    --kunorpus "${KUNORPUS}" \
    --kunorpus-count "${KUNORPUS_COUNT}" \
    --kunorpus-seed-window "${KUNORPUS_SEED_WINDOW}" \
    --kunorpus-max-transfer-lamports "${KUNORPUS_MAX_TRANSFER_LAMPORTS}" \
    --recent-blockhash "${blockhash}" \
    --from-seed-index "${FROM_GENESIS_ACCOUNT}" \
    --to-seed-start "${TO_GENESIS_ACCOUNT_ONE}"
}

materialize_generated_bundle_scenario() {
  local seed="$1"
  local recipe="$2"
  local output="$3"
  local packet_dir="$4"
  local metadata="$5"
  local blockhash="$6"
  "${MATERIALIZE_SCENARIO}" \
    --shape-seed "${seed}" \
    --shape-max-batches "${GENERATED_BUNDLE_MAX_BATCHES}" \
    --shape-max-packets "${GENERATED_BUNDLE_MAX_PACKETS}" \
    --recipe-output "${recipe}" \
    --output "${output}" \
    --packet-dir "${packet_dir}" \
    --metadata-output "${metadata}" \
    --gen-simple-system-txnctx "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --gen-program-invocation "${GEN_PROGRAM_INVOCATION_BIN}" \
    --bridge "${BRIDGE_BIN}" \
    --kunorpus "${KUNORPUS}" \
    --kunorpus-count "${KUNORPUS_COUNT}" \
    --kunorpus-seed-window "${KUNORPUS_SEED_WINDOW}" \
    --kunorpus-max-transfer-lamports "${KUNORPUS_MAX_TRANSFER_LAMPORTS}" \
    --recent-blockhash "${blockhash}" \
    --from-seed-index "${FROM_GENESIS_ACCOUNT}" \
    --to-seed-start "${TO_GENESIS_ACCOUNT_ONE}" \
    --vary-recipient
}

require_pattern() {
  local file="$1"
  local pattern="$2"
  local description="$3"
  local timeout_secs="$4"
  if wait_for_pattern "${file}" "${pattern}" "${timeout_secs}"; then
    echo "  ok: ${description}"
  else
    echo "  missing: ${description}" >&2
    echo "  --- tail ${file} ---" >&2
    tail -n 80 "${file}" >&2 || true
    exit 1
  fi
}

require_pattern_count() {
  local file="$1"
  local pattern="$2"
  local expected_count="$3"
  local description="$4"
  local timeout_secs="$5"
  if wait_for_pattern_count "${file}" "${pattern}" "${expected_count}" "${timeout_secs}"; then
    echo "  ok: ${description}"
  else
    local observed=0
    observed=$(grep -cE "${pattern}" "${file}" 2>/dev/null || true)
    echo "  missing: ${description} (observed ${observed}, expected ${expected_count})" >&2
    echo "  --- tail ${file} ---" >&2
    tail -n 80 "${file}" >&2 || true
    exit 1
  fi
}

require_no_post_generation_batch_results() {
  local bam_log="$1"
  local seq_start="$2"
  local seq_count="$3"
  local description="$4"
  local seq_end=$((seq_start + seq_count))
  local line
  local leak=""
  local result_re='scheduler<-validator batch_result seq_id=([0-9]+).* conn=([0-9]+)'
  while IFS= read -r line; do
    if [[ "${line}" =~ ${result_re} ]]; then
      local seq_id="${BASH_REMATCH[1]}"
      local conn_id="${BASH_REMATCH[2]}"
      if (( seq_id >= seq_start && seq_id < seq_end && conn_id >= 2 )); then
        leak="${line}"
        break
      fi
    fi
  done < "${bam_log}"
  if [[ -n "${leak}" ]]; then
    echo "  stale-generation leak: ${description}" >&2
    echo "  leaked result: ${leak}" >&2
    echo "  --- tail ${bam_log} ---" >&2
    tail -n 80 "${bam_log}" >&2 || true
    exit 1
  fi
  echo "  ok: ${description}"
}

wait_for_exact_balance() {
  local pubkey="$1"
  local expected="$2"
  local timeout_secs="$3"
  local observed=""
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    observed=$(rpc_balance "${pubkey}" 2>/dev/null || true)
    if [[ "${observed}" == "${expected}" ]]; then
      printf '%s\n' "${observed}"
      return 0
    fi
    sleep 1
  done
  observed=$(rpc_balance "${pubkey}" 2>/dev/null || true)
  printf '%s\n' "${observed}"
  return 1
}

wait_for_balance_below() {
  local pubkey="$1"
  local maximum="$2"
  local timeout_secs="$3"
  local observed=""
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    observed=$(rpc_balance "${pubkey}" 2>/dev/null || true)
    if [[ "${observed}" =~ ^[0-9]+$ ]] && (( observed < maximum )); then
      printf '%s\n' "${observed}"
      return 0
    fi
    sleep 1
  done
  observed=$(rpc_balance "${pubkey}" 2>/dev/null || true)
  printf '%s\n' "${observed}"
  return 1
}

wait_for_balance_above() {
  local pubkey="$1"
  local minimum="$2"
  local timeout_secs="$3"
  local observed=""
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    observed=$(rpc_balance "${pubkey}" 2>/dev/null || true)
    if [[ "${observed}" =~ ^[0-9]+$ ]] && (( observed > minimum )); then
      printf '%s\n' "${observed}"
      return 0
    fi
    sleep 1
  done
  observed=$(rpc_balance "${pubkey}" 2>/dev/null || true)
  printf '%s\n' "${observed}"
  return 1
}

wait_for_account_owner_space() {
  local pubkey="$1"
  local expected_owner="$2"
  local expected_space="$3"
  local timeout_secs="$4"
  local observed_owner=""
  local observed_space=""
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    local info
    info=$(rpc_account_info "${pubkey}" 2>/dev/null || true)
    observed_owner=$(printf '%s' "${info}" | jq -r '.result.value.owner // empty' 2>/dev/null || true)
    observed_space=$(printf '%s' "${info}" | jq -r '.result.value.space // empty' 2>/dev/null || true)
    if [[ "${observed_owner}" == "${expected_owner}" && "${observed_space}" == "${expected_space}" ]]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

write_normalized_outcome() {
  local iter_dir="$1"
  local iter_mode="$2"
  local iter_input_family="$3"
  local runner_kind="$4"

  "${NORMALIZE_OUTCOME}" \
    --iter-dir "${iter_dir}" \
    --target "${TARGET_NAME}" \
    --runner-kind "${runner_kind}" \
    --mode "${iter_mode}" \
    --input-family "${iter_input_family}"
  local check_args=(
    "${iter_dir}/normalized_outcome.json"
    --output "${iter_dir}/outcome_self_check.json"
  )
  if [[ "${iter_mode}" == "bam_restart_churn" ]]; then
    check_args+=( --allow-m42-duplicate-terminal )
  fi
  if ! "${CHECK_OUTCOME}" "${check_args[@]}"; then
    if [[ "${ALLOW_SELF_CHECK_FAILURES}" == "1" ]]; then
      SELF_CHECK_FAILURES=$((SELF_CHECK_FAILURES + 1))
      echo "warning: preserving failed outcome self-check for differential classification: ${iter_dir}/outcome_self_check.json" >&2
      return 0
    fi
    die "outcome self-check failed; see ${iter_dir}/outcome_self_check.json"
  fi
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
    system_kind)
      echo "${line}" | sed -n 's/.* system_kind=\([^ ]*\) from=.*/\1/p'
      ;;
    owner)
      echo "${line}" | sed -n 's/.* owner=\([^ ]*\) system_ix=.*/\1/p'
      ;;
    space)
      echo "${line}" | sed -n 's/.* space=\([0-9][0-9]*\) owner=.*/\1/p'
      ;;
    node)
      echo "${line}" | sed -n 's/.* node=\([^ ]*\) vote=.*/\1/p'
      ;;
    vote)
      echo "${line}" | sed -n 's/.* vote=\([^ ]*\) authorized_voter=.*/\1/p'
      ;;
    authorized_voter)
      echo "${line}" | sed -n 's/.* authorized_voter=\([^ ]*\) vote_slot=.*/\1/p'
      ;;
    vote_slot)
      echo "${line}" | sed -n 's/.* vote_slot=\([0-9][0-9]*\) vote_hash=.*/\1/p'
      ;;
    vote_hash)
      echo "${line}" | sed -n 's/.* vote_hash=\([^ ]*\) recent_blockhash=.*/\1/p'
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
    system_kind)
      echo "${line}" | sed -n 's/.* system_kind=\([^ ]*\) .*/\1/p'
      ;;
    *)
      die "unsupported generator field ${field}"
      ;;
  esac
}

choose_mode() {
  local iter_seed="$1"
  local iter_idx="${2:-0}"
  if [[ -n "${MODE_LIST}" ]]; then
    local -a listed_modes=()
    local IFS=,
    read -r -a listed_modes <<< "${MODE_LIST}"
    (( ${#listed_modes[@]} )) || die "--mode-list did not contain any modes"
    printf '%s\n' "${listed_modes[$(( iter_idx % ${#listed_modes[@]} ))]}"
    return 0
  fi

  if [[ "${MODE}" != "random" ]]; then
    printf '%s\n' "${MODE}"
    return 0
  fi

  case "${INPUT_FAMILY}" in
    kunorpus_vote)
      printf '%s\n' vote_reject_once
      return 0
      ;;
    kunorpus_raw_txn)
      printf '%s\n' raw_kunorpus_once
      return 0
      ;;
    kunorpus_bundle)
      printf '%s\n' generated_bundle
      return 0
      ;;
  esac

  local modes=(
    commit_once
    replay_same_conn
    replay_after_reconnect
    unique_after_reconnect
    seq_id_wrap_sequence
    seq_id_out_of_order_multi_batch
    seq_id_wrap_out_of_order_multi_batch
    seq_id_wrap_conflicting_spend_multi_batch
    partial_drain_reconnect
    queue_burst_reconnect
    queue_burst64_reconnect
    queue_burst64_leader_plus1_reconnect
    schedule_boundary_jitter
    queue_reconnect_timing_jitter
    queue_burst_multi_reconnect
    queue_burst128_reconnect
    queue_burst256_reconnect
    source_mix_bam_tpu
    source_mix_precommit
    source_mix_atomic_revert_precommit
    source_mix_duplicate_tpu_after_bam
    disable_enable_tpu_release
    source_mix_queue_burst_reconnect
    source_mix_queue_burst_multi_reconnect
    disable_enable_queue_burst_reconnect
    seq_id_max_once
    seq_id_max_replay_after_reconnect
	    duplicate_seq_split
    duplicate_seq_split_reconnect
    seq_collision_same_conn
    seq_collision_reconnect
    mixed_multi_batch
    mixed_empty_multi_batch
    mixed_malformed_multi_batch
    mixed_bad_signature_multi_batch
    mixed_bad_signature_reconnect
    mixed_stale_multi_batch
    mixed_stale_reconnect
    mixed_terminal_producers_reconnect
    random_mixed_multi_batch
    generated_bundle
    stale_slot_reject
    stale_slot_reject_reconnect
    empty_batch_reject
    empty_batch_reject_reconnect
    malformed_first_atomic
    malformed_first_atomic_reconnect
    malformed_tail_atomic
    malformed_tail_atomic_reconnect
    bad_signature_first_atomic
    bad_signature_first_atomic_reconnect
    bad_signature_tail_atomic
    bad_signature_tail_atomic_reconnect
    non_atomic_single_packet
    valid_alt_commit
    invalid_alt_missing_table
    invalid_alt_missing_table_reconnect
    bam_fee_priority_commit
    bam_fee_priority_replay_after_reconnect
    bam_fee_queue_burst_reconnect
    bam_fee_source_mix_queue_burst_reconnect
    bam_fee_config_refresh_queue_burst
    bam_fee_config_refresh_source_mix_queue_burst
    bam_fee_config_midqueue_refresh
    bam_fee_config_midqueue_source_mix_queue_burst
    bam_fee_config_midqueue_source_mix_multi_reconnect
    fee_only_commit
    fee_only_reconnect
    durable_nonce_commit
    durable_nonce_reconnect
    durable_nonce_replay_after_reconnect
    durable_nonce_wrong_authority
    durable_nonce_wrong_authority_reconnect
    bam_cu_limit_fail
    bam_cu_limit_fail_reconnect
    atomic_revert
    atomic_revert_reconnect
    atomic_first_overdraft
    atomic_first_overdraft_reconnect
    atomic_mid_fail
    atomic_mid_fail_reconnect
    atomic_blockhash_mid_fail
    atomic_blockhash_mid_fail_reconnect
    atomic_resolver_mid_fail
    atomic_resolver_mid_fail_reconnect
    atomic_duplicate_sig_mid_fail
    atomic_duplicate_sig_mid_fail_reconnect
    non_atomic_inconsistent_bundle
    non_atomic_first_overdraft
    non_atomic_mid_overdraft
    non_atomic_partial_overdraft
    non_atomic_partial_overdraft_reconnect
    non_atomic_partial_resolver_fail
    non_atomic_partial_duplicate_sig
    non_atomic_partial_cu_fail
  )
  if [[ "${INPUT_FAMILY}" == "random" ]]; then
    modes+=(
      vote_reject_once
      vote_reject_reconnect
      raw_kunorpus_once
      raw_kunorpus_reconnect
    )
  fi
  if [[ "${ALLOW_OPERATOR_PERTURBATIONS}" == "1" ]]; then
    modes+=(
      disable_enable_unique_after_reconnect
      url_churn_unique_after_reconnect
      fd_pause_resume_churn
      bam_pause_resume_churn
      fd_restart_churn
      bam_restart_churn
      url_sni_churn
    )
  fi
  printf '%s\n' "${modes[$(( iter_seed % ${#modes[@]} ))]}"
}

choose_input_family() {
  local iter_seed="$1"
  local iter_mode="${2:-}"
  if [[ "${iter_mode}" == "external_scenario" ]]; then
    printf '%s\n' external_scenario
    return 0
  fi
  if [[ "${iter_mode}" == "generated_bundle" ]]; then
    printf '%s\n' kunorpus_bundle
    return 0
  fi
  if [[ "${iter_mode}" == "non_atomic_inconsistent_bundle" || "${iter_mode}" == "non_atomic_first_overdraft" || "${iter_mode}" == "non_atomic_mid_overdraft" || "${iter_mode}" == "non_atomic_partial_overdraft" || "${iter_mode}" == "non_atomic_partial_overdraft_reconnect" || "${iter_mode}" == "non_atomic_partial_resolver_fail" || "${iter_mode}" == "non_atomic_partial_blockhash_fail" || "${iter_mode}" == "non_atomic_partial_duplicate_sig" || "${iter_mode}" == "non_atomic_partial_cu_fail" || "${iter_mode}" == "seq_id_wrap_conflicting_spend_multi_batch" || "${iter_mode}" == "atomic_revert" || "${iter_mode}" == "atomic_revert_reconnect" || "${iter_mode}" == "atomic_first_overdraft" || "${iter_mode}" == "atomic_first_overdraft_reconnect" || "${iter_mode}" == "source_mix_atomic_revert_precommit" || "${iter_mode}" == "atomic_mid_fail" || "${iter_mode}" == "atomic_mid_fail_reconnect" || "${iter_mode}" == "atomic_blockhash_mid_fail" || "${iter_mode}" == "atomic_blockhash_mid_fail_reconnect" || "${iter_mode}" == "atomic_resolver_mid_fail" || "${iter_mode}" == "atomic_resolver_mid_fail_reconnect" || "${iter_mode}" == "atomic_duplicate_sig_mid_fail" || "${iter_mode}" == "atomic_duplicate_sig_mid_fail_reconnect" || "${iter_mode}" == "empty_batch_reject_reconnect" || "${iter_mode}" == "malformed_first_atomic_reconnect" || "${iter_mode}" == "malformed_tail_atomic_reconnect" || "${iter_mode}" == "bad_signature_first_atomic_reconnect" || "${iter_mode}" == "bad_signature_tail_atomic_reconnect" ]]; then
    printf '%s\n' synthetic
    return 0
  fi
  if [[ "${iter_mode}" == "vote_reject_once" || "${iter_mode}" == "vote_reject_reconnect" ]]; then
    printf '%s\n' kunorpus_vote
    return 0
  fi
  if [[ "${iter_mode}" == "raw_kunorpus_once" || "${iter_mode}" == "raw_kunorpus_reconnect" ]]; then
    printf '%s\n' kunorpus_raw_txn
    return 0
  fi
  if [[ "${iter_mode}" == "partial_drain_reconnect" || "${iter_mode}" == "queue_burst_reconnect" || "${iter_mode}" == "queue_burst64_reconnect" || "${iter_mode}" == "queue_burst64_leader_plus1_reconnect" || "${iter_mode}" == "schedule_boundary_jitter" || "${iter_mode}" == "queue_reconnect_timing_jitter" || "${iter_mode}" == "queue_burst_multi_reconnect" || "${iter_mode}" == "queue_burst128_reconnect" || "${iter_mode}" == "queue_burst256_reconnect" || "${iter_mode}" == "queue_burst512_reconnect" || "${iter_mode}" == "queue_burst_leader_reconnect" || "${iter_mode}" == "queue_burst64_leader_reconnect" || "${iter_mode}" == "bam_fee_queue_burst_reconnect" || "${iter_mode}" == "bam_fee_source_mix_queue_burst_reconnect" || "${iter_mode}" == "bam_fee_config_refresh_queue_burst" || "${iter_mode}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${iter_mode}" == "bam_fee_config_midqueue_refresh" || "${iter_mode}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${iter_mode}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" || "${iter_mode}" == "source_mix_bam_tpu" || "${iter_mode}" == "source_mix_precommit" || "${iter_mode}" == "source_mix_atomic_revert_precommit" || "${iter_mode}" == "source_mix_duplicate_tpu_after_bam" || "${iter_mode}" == "disable_enable_tpu_release" || "${iter_mode}" == "source_mix_queue_burst_reconnect" || "${iter_mode}" == "source_mix_queue_burst_multi_reconnect" || "${iter_mode}" == "disable_enable_queue_burst_reconnect" || "${iter_mode}" == "quarantine_disable_enable_queue_inflight" || "${iter_mode}" == "quarantine_url_churn_queue_inflight" || "${iter_mode}" == "valid_alt_commit" || "${iter_mode}" == "invalid_alt_missing_table" || "${iter_mode}" == "invalid_alt_missing_table_reconnect" || "${iter_mode}" == "mixed_terminal_producers_reconnect" || "${iter_mode}" == "bam_fee_priority_commit" || "${iter_mode}" == "bam_fee_priority_replay_after_reconnect" || ( "${iter_mode}" == "bam_fee_url_churn_priority_commit" || "${iter_mode}" == "bam_fee_url_churn_same_slot_priority_commit" || "${iter_mode}" == "bam_fee_config_refresh_priority_commit" || "${iter_mode}" == "bam_fee_config_commission_refresh_priority_commit" ) || "${iter_mode}" == "fee_only_commit" || "${iter_mode}" == "fee_only_reconnect" || "${iter_mode}" == "durable_nonce_commit" || "${iter_mode}" == "durable_nonce_reconnect" || "${iter_mode}" == "durable_nonce_replay_after_reconnect" || "${iter_mode}" == "durable_nonce_wrong_authority" || "${iter_mode}" == "durable_nonce_wrong_authority_reconnect" || "${iter_mode}" == "bam_cu_limit_fail" || "${iter_mode}" == "bam_cu_limit_fail_reconnect" || "${iter_mode}" == "seq_id_max_once" || "${iter_mode}" == "seq_id_max_replay_after_reconnect" || "${iter_mode}" == "seq_id_wrap_sequence" || "${iter_mode}" == "duplicate_seq_split" || "${iter_mode}" == "duplicate_seq_split_reconnect" || "${iter_mode}" == "fd_pause_resume_churn" || "${iter_mode}" == "bam_pause_resume_churn" || "${iter_mode}" == "fd_restart_churn" || "${iter_mode}" == "bam_restart_churn" || "${iter_mode}" == "url_sni_churn" ]]; then
    printf '%s\n' synthetic
    return 0
  fi
  if [[ "${INPUT_FAMILY}" != "random" ]]; then
    printf '%s\n' "${INPUT_FAMILY}"
    return 0
  fi

  local families=(
    synthetic
    kunorpus_system
  )
  printf '%s\n' "${families[$(( iter_seed % ${#families[@]} ))]}"
}

write_repro() {
  local iter_dir="$1"
  local iter_seed="$2"
  local iter_mode="$3"
  local iter_input_family="$4"
  cat >"${iter_dir}/repro.sh" <<EOF2
#!/usr/bin/env bash
set -euo pipefail
IFS=\$'\\n\\t'
  USE_SUDO=${USE_SUDO@Q} FDCTL_BIN=${FDCTL_BIN@Q} SOLANA_BIN_DIR=${SOLANA_BIN_DIR@Q} GUI_URL=${GUI_URL@Q} METRICS_URL=${METRICS_URL@Q} CHECK_BAM_SHRED=${CHECK_BAM_SHRED@Q} BAM_SERVER_TLS_CERT=${BAM_SERVER_TLS_CERT@Q} BAM_SERVER_TLS_KEY=${BAM_SERVER_TLS_KEY@Q} BAM_PRESEEDED_NONCE_HASH_SEED_BASE=${BAM_PRESEEDED_NONCE_HASH_SEED_BASE@Q} BAM_PRESEEDED_PROGRAM_ELF=${BAM_PRESEEDED_PROGRAM_ELF@Q} BAM_PRESEEDED_PROGRAM_ID=${BAM_PRESEEDED_PROGRAM_ID@Q} FULLFD_SNAPSHOT_FILE=${FULLFD_SNAPSHOT_FILE@Q} FULLFD_SNAPSHOT_DIR=${FULLFD_SNAPSHOT_DIR@Q} FULLFD_SNAPSHOT_BOOTSTRAP_CONFIG=${FULLFD_SNAPSHOT_BOOTSTRAP_CONFIG@Q} FULLFD_SNAPSHOT_GENESIS_FILE=${FULLFD_SNAPSHOT_GENESIS_FILE@Q} FULLFD_SNAPSHOT_GENESIS_DEST=${FULLFD_SNAPSHOT_GENESIS_DEST@Q} QUEUE_BURST_BATCH_COUNT=${QUEUE_BURST_BATCH_COUNT@Q} QUEUE_BURST_MAX_SCHEDULE_SLOT=${QUEUE_BURST_MAX_SCHEDULE_SLOT@Q} QUEUE_BURST_CLOSE_AFTER_RESULTS=${QUEUE_BURST_CLOSE_AFTER_RESULTS@Q} NORMAL_TPU_BURST_COUNT=${NORMAL_TPU_BURST_COUNT@Q} NORMAL_TPU_MATRIX_COUNT=${NORMAL_TPU_MATRIX_COUNT@Q} BAM_DISABLED_HOLD_MS=${BAM_DISABLED_HOLD_MS@Q} BAM_DISABLED_WORKLOAD_SCRIPT=${BAM_DISABLED_WORKLOAD_SCRIPT@Q} ${ROOT}/contrib/fuzz-local-bam-stateful.sh --config ${CONFIG@Q} --rpc-url ${RPC_URL@Q} --target-name ${TARGET_NAME@Q} --fddev-bin ${FDDEV@Q} --runner-kind ${RUNNER_KIND@Q} --bam-bind ${BAM_BIND@Q} --bam-url ${BAM_URL@Q} --bam-bad-url ${BAM_BAD_URL@Q} --bam-tpu-port ${BAM_TPU_PORT} --bam-tpu-fwd-port ${BAM_TPU_FWD_PORT} --bam-shred-port ${BAM_SHRED_PORT} --iterations 1 --seed ${iter_seed} --mode ${iter_mode} --input-family ${iter_input_family} --kunorpus-count ${KUNORPUS_COUNT} --kunorpus-seed-window ${KUNORPUS_SEED_WINDOW} --kunorpus-max-transfer-lamports ${KUNORPUS_MAX_TRANSFER_LAMPORTS} --kunorpus-system-kind ${KUNORPUS_SYSTEM_KIND} --generated-bundle-max-batches ${GENERATED_BUNDLE_MAX_BATCHES} --generated-bundle-max-packets ${GENERATED_BUNDLE_MAX_PACKETS} --timeout-secs ${TIMEOUT_SECS} --log-dir ${iter_dir@Q} --from-genesis-account ${FROM_GENESIS_ACCOUNT} --to-genesis-account-one ${TO_GENESIS_ACCOUNT_ONE} --to-genesis-account-two ${TO_GENESIS_ACCOUNT_TWO} $( [[ -n "${EXTERNAL_SCENARIO_FILE}" ]] && printf ' --scenario-file %q' "${EXTERNAL_SCENARIO_FILE}" ) $( [[ -n "${EXTERNAL_OPERATOR_EVENTS_FILE}" ]] && printf ' --operator-events-file %q' "${EXTERNAL_OPERATOR_EVENTS_FILE}" ) $( [[ "${ALLOW_OPERATOR_PERTURBATIONS}" == "1" ]] && printf '%s' '--allow-operator-perturbations' ) $( [[ "${ALLOW_SELF_CHECK_FAILURES}" == "1" ]] && printf '%s' '--allow-self-check-failures' ) $( [[ -n "${LIVE_COVERAGE_DIR}" ]] && printf ' --live-coverage-dir %q' "${LIVE_COVERAGE_DIR}" )
EOF2
  chmod +x "${iter_dir}/repro.sh"
}

start_fddev() {
  local iter_dir="$1"
  local fd_log="$2"
  local configure_log="${iter_dir}/configure.log"
  CURRENT_FD_PID_FILE="${iter_dir}/fd.pid"
  rm -f "${CURRENT_FD_PID_FILE}"
  : >"${configure_log}"

  CONFIG_INITIALIZED=1
  if [[ -n "${FULLFD_SNAPSHOT_FILE}" ]]; then
    [[ "${RUNNER_KIND}" == "fullfd" ]] \
      || die "FULLFD_SNAPSHOT_FILE is only valid with --runner-kind fullfd"
    [[ -r "${FULLFD_SNAPSHOT_FILE}" ]] \
      || die "missing full Firedancer snapshot ${FULLFD_SNAPSHOT_FILE}"
    [[ -n "${FULLFD_SNAPSHOT_DIR}" ]] \
      || die "FULLFD_SNAPSHOT_DIR is required with FULLFD_SNAPSHOT_FILE"
    [[ -r "${FULLFD_SNAPSHOT_BOOTSTRAP_CONFIG}" ]] \
      || die "missing snapshot bootstrap config ${FULLFD_SNAPSHOT_BOOTSTRAP_CONFIG}"
    [[ -r "${FULLFD_SNAPSHOT_GENESIS_FILE}" ]] \
      || die "missing snapshot-matched genesis ${FULLFD_SNAPSHOT_GENESIS_FILE}"
    [[ -n "${FULLFD_SNAPSHOT_GENESIS_DEST}" ]] \
      || die "FULLFD_SNAPSHOT_GENESIS_DEST is required with FULLFD_SNAPSHOT_FILE"

    local snapshot_bootstrap_env=( env )
    if [[ -n "${LIVE_COVERAGE_DIR}" ]]; then
      snapshot_bootstrap_env+=( "LLVM_PROFILE_FILE=${LIVE_COVERAGE_DIR}/${TARGET_NAME}-${ITER_MODE:-startup}-snapshot-bootstrap-%m-%p.profraw" )
    fi
    run_logged "snapshot bootstrap configure init" "${configure_log}" \
      "${SUDO[@]}" "${snapshot_bootstrap_env[@]}" "${FDDEV}" configure init all \
      --config "${FULLFD_SNAPSHOT_BOOTSTRAP_CONFIG}"
  fi
  local configure_env=( env )
  if [[ -n "${LIVE_COVERAGE_DIR}" ]]; then
    configure_env+=( "LLVM_PROFILE_FILE=${LIVE_COVERAGE_DIR}/${TARGET_NAME}-${ITER_MODE:-startup}-configure-init-%m-%p.profraw" )
  fi
  if [[ "${RUNNER_KIND}" == "fullfd" && -n "${BAM_PRESEEDED_PROGRAM_ELF}" ]]; then
    [[ -r "${BAM_PRESEEDED_PROGRAM_ELF}" ]] || die "missing preseeded program ELF ${BAM_PRESEEDED_PROGRAM_ELF}"
    [[ -n "${BAM_PRESEEDED_PROGRAM_ID}" ]] || die "BAM_PRESEEDED_PROGRAM_ID is required with BAM_PRESEEDED_PROGRAM_ELF"
    configure_env+=(
      "FD_DEV_PRESEED_BAM_PROGRAM_ELF=${BAM_PRESEEDED_PROGRAM_ELF}"
      "FD_DEV_PRESEED_BAM_PROGRAM_ID=${BAM_PRESEEDED_PROGRAM_ID}"
    )
  fi
  if [[ "${ITER_MODE:-}" == "valid_alt_commit" ]]; then
    run_logged "validator configure init" "${configure_log}" \
      "${SUDO[@]}" "${configure_env[@]}" \
      FD_DEV_PRESEED_BAM_ALT=1 \
      FD_DEV_PRESEED_BAM_ALT_TABLE_SEED="${BAM_PRESEEDED_ALT_TABLE_SEED}" \
      FD_DEV_PRESEED_BAM_ALT_ADDRESS_SEED="${TO_GENESIS_ACCOUNT_ONE}" \
      "${FDDEV}" configure init all --config "${CONFIG}"
  elif [[ ( "${ITER_MODE:-}" == "durable_nonce_commit" || "${ITER_MODE:-}" == "durable_nonce_reconnect" || "${ITER_MODE:-}" == "durable_nonce_replay_after_reconnect" || "${ITER_MODE:-}" == "durable_nonce_wrong_authority" || "${ITER_MODE:-}" == "durable_nonce_wrong_authority_reconnect" ) && "${RUNNER_KIND}" == "fullfd" ]]; then
    local nonce_seed=$((900000 + ITER_SEED))
    local nonce_hash_seed=$((BAM_PRESEEDED_NONCE_HASH_SEED_BASE + ITER_SEED))
    run_logged "validator configure init" "${configure_log}" \
      "${SUDO[@]}" "${configure_env[@]}" \
      FD_DEV_PRESEED_BAM_NONCE=1 \
      FD_DEV_PRESEED_BAM_NONCE_ACCOUNT_SEED="${nonce_seed}" \
      FD_DEV_PRESEED_BAM_NONCE_AUTH_SEED="${FROM_GENESIS_ACCOUNT}" \
      FD_DEV_PRESEED_BAM_NONCE_HASH_SEED="${nonce_hash_seed}" \
      "${FDDEV}" configure init all --config "${CONFIG}"
  else
    run_logged "validator configure init" "${configure_log}" \
      "${SUDO[@]}" "${configure_env[@]}" "${FDDEV}" configure init all --config "${CONFIG}"
  fi

  if [[ -n "${FULLFD_SNAPSHOT_FILE}" ]]; then
    "${SUDO[@]}" cp "${FULLFD_SNAPSHOT_GENESIS_FILE}" \
      "${FULLFD_SNAPSHOT_GENESIS_DEST}"
    "${SUDO[@]}" chmod 0600 "${FULLFD_SNAPSHOT_GENESIS_DEST}"
    "${SUDO[@]}" mkdir -p "${FULLFD_SNAPSHOT_DIR}"
    "${SUDO[@]}" cp "${FULLFD_SNAPSHOT_FILE}" \
      "${FULLFD_SNAPSHOT_DIR}/${FULLFD_SNAPSHOT_FILE##*/}"
    "${SUDO[@]}" chmod 0644 \
      "${FULLFD_SNAPSHOT_DIR}/${FULLFD_SNAPSHOT_FILE##*/}"
  fi

  local configure_check_env=( env )
  if [[ -n "${LIVE_COVERAGE_DIR}" ]]; then
    configure_check_env+=( "LLVM_PROFILE_FILE=${LIVE_COVERAGE_DIR}/${TARGET_NAME}-${ITER_MODE:-startup}-configure-check-%m-%p.profraw" )
  fi
  if [[ "${RUNNER_KIND}" == "fullfd" && -n "${BAM_PRESEEDED_PROGRAM_ELF}" ]]; then
    configure_check_env+=(
      "FD_DEV_PRESEED_BAM_PROGRAM_ELF=${BAM_PRESEEDED_PROGRAM_ELF}"
      "FD_DEV_PRESEED_BAM_PROGRAM_ID=${BAM_PRESEEDED_PROGRAM_ID}"
    )
  fi
  if [[ "${ITER_MODE:-}" == "valid_alt_commit" ]]; then
    configure_check_env+=(
      "FD_DEV_PRESEED_BAM_ALT=1"
      "FD_DEV_PRESEED_BAM_ALT_TABLE_SEED=${BAM_PRESEEDED_ALT_TABLE_SEED}"
      "FD_DEV_PRESEED_BAM_ALT_ADDRESS_SEED=${TO_GENESIS_ACCOUNT_ONE}"
    )
  elif [[ ( "${ITER_MODE:-}" == "durable_nonce_commit" || "${ITER_MODE:-}" == "durable_nonce_reconnect" || "${ITER_MODE:-}" == "durable_nonce_replay_after_reconnect" || "${ITER_MODE:-}" == "durable_nonce_wrong_authority" || "${ITER_MODE:-}" == "durable_nonce_wrong_authority_reconnect" ) && "${RUNNER_KIND}" == "fullfd" ]]; then
    configure_check_env+=(
      "FD_DEV_PRESEED_BAM_NONCE=1"
      "FD_DEV_PRESEED_BAM_NONCE_ACCOUNT_SEED=$((900000 + ITER_SEED))"
      "FD_DEV_PRESEED_BAM_NONCE_AUTH_SEED=${FROM_GENESIS_ACCOUNT}"
      "FD_DEV_PRESEED_BAM_NONCE_HASH_SEED=$((BAM_PRESEEDED_NONCE_HASH_SEED_BASE + ITER_SEED))"
    )
  fi
  if [[ "${RUNNER_KIND}" == "fullfd" ]]; then
    run_logged_retry "validator configure check" "${configure_log}" 10 1 \
      "${SUDO[@]}" "${configure_check_env[@]}" "${FDDEV}" configure check all --config "${CONFIG}"
  else
    run_logged "validator configure check" "${configure_log}" \
      "${SUDO[@]}" "${configure_check_env[@]}" "${FDDEV}" configure check all --config "${CONFIG}"
  fi

  local validator_env=()
  if [[ -n "${LIVE_COVERAGE_DIR}" ]]; then
    validator_env=( env "LLVM_PROFILE_FILE=${LIVE_COVERAGE_DIR}/${TARGET_NAME}-parent-%m-%p.profraw" )
  fi

  "${SUDO[@]}" "${validator_env[@]}" bash -lc '
    pid_file="$1"
    fddev_bin="$2"
    config_path="$3"
    log_file="$4"
    setsid "${fddev_bin}" dev --no-watch --no-configure --config "${config_path}" > "${log_file}" 2>&1 &
    child_pid=$!
    echo "${child_pid}" > "${pid_file}"
    wait "${child_pid}"
  ' bash "${CURRENT_FD_PID_FILE}" "${FDDEV}" "${CONFIG}" "${fd_log}" &
  FD_SUPERVISOR_PID=$!

  FD_PID=""
  for _ in $(seq 1 50); do
    if [[ -s "${CURRENT_FD_PID_FILE}" ]]; then
      FD_PID=$(tr -d '[:space:]' < "${CURRENT_FD_PID_FILE}")
      break
    fi
    sleep 0.2
  done
  [[ -n "${FD_PID}" ]] || die "failed to capture fddev pid"
}

wait_rpc_ready() {
  local timeout_secs="$1"
  local blockhash=""
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    if blockhash=$(rpc_latest_blockhash 2>/dev/null); then
      printf '%s\n' "${blockhash}"
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

bridge_packet() {
  local input_path="$1"
  local output_path="$2"
  local bridge_out="$3"
  local blockhash="$4"
  local to_index="$5"
  "${BRIDGE_BIN}" \
    --input "${input_path}" \
    --output "${output_path}" \
    --adapt-mode local-system-transfer \
    --recent-blockhash "${blockhash}" \
    --from-genesis-account "${FROM_GENESIS_ACCOUNT}" \
    --to-genesis-account "${to_index}" \
    >"${bridge_out}"
}

prepare_normal_tpu_coverage_matrix() {
  local iter_dir="$1"
  local blockhash="$2"
  local count="$3"
  local -a system_kinds=(
    transfer
    fee-only
    assign
    allocate
    create-account
    transfer-with-seed
    create-account-with-seed
  )

  NORMAL_TPU_MATRIX_DIR="${iter_dir}/normal-tpu-matrix"
  mkdir -p "${NORMAL_TPU_MATRIX_DIR}"
  : >"${NORMAL_TPU_MATRIX_DIR}/manifest.txt"

  local i kind input_path packet_path gen_log bridge_log to_index
  local -a extra_args
  for ((i=0; i<count; i++)); do
    kind="${system_kinds[$((i % ${#system_kinds[@]}))]}"
    input_path=$(printf '%s/input-%03d.txnctx' "${NORMAL_TPU_MATRIX_DIR}" "${i}")
    packet_path=$(printf '%s/packet-%03d.packet' "${NORMAL_TPU_MATRIX_DIR}" "${i}")
    gen_log=$(printf '%s/gen-%03d.log' "${NORMAL_TPU_MATRIX_DIR}" "${i}")
    bridge_log=$(printf '%s/bridge-%03d.log' "${NORMAL_TPU_MATRIX_DIR}" "${i}")
    to_index=$((100 + i))
    extra_args=(
      --system-kind "${kind}"
      --lamports "$((2000000 + i))"
      --cu-limit "$((50000 + (i % 5) * 50000))"
      --cu-price "$((1000 + i * 37))"
    )
    if [[ "${kind}" == "allocate" || "${kind}" == "create-account" || "${kind}" == "create-account-with-seed" ]]; then
      extra_args+=( --space "$((64 + (i % 4) * 64))" )
    fi
    if (( i % 3 == 0 )); then
      extra_args+=( --heap-frame "$((32768 + (i % 8) * 1024))" )
    fi
    if (( i % 5 == 0 )); then
      extra_args+=( --loaded-accounts-data-size-limit "$((65536 + i * 128))" )
    fi

    "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
      --output "${input_path}" \
      "${extra_args[@]}" \
      >"${gen_log}"
    bridge_packet "${input_path}" "${packet_path}" "${bridge_log}" "${blockhash}" "${to_index}"
    printf 'index=%s kind=%s to_seed_index=%s packet=%s\n' \
      "${i}" "${kind}" "${to_index}" "${packet_path}" \
      >>"${NORMAL_TPU_MATRIX_DIR}/manifest.txt"
  done
}

write_max_schedule_slot_toml() {
  local value="$1"
  if [[ "${value}" =~ ^[0-9]+$ ]]; then
    printf 'max_schedule_slot = %s\n' "${value}"
  else
    printf 'max_schedule_slot = "%s"\n' "${value}"
  fi
}

is_queue_burst_mode() {
  case "$1" in
    partial_drain_reconnect|queue_burst_reconnect|queue_burst64_reconnect|queue_burst64_leader_plus1_reconnect|schedule_boundary_jitter|queue_reconnect_timing_jitter|queue_burst_multi_reconnect|source_mix_queue_burst_multi_reconnect|queue_burst128_reconnect|queue_burst256_reconnect|queue_burst512_reconnect|queue_burst_leader_reconnect|queue_burst64_leader_reconnect|bam_fee_queue_burst_reconnect|bam_fee_source_mix_queue_burst_reconnect|bam_fee_config_refresh_queue_burst|bam_fee_config_refresh_source_mix_queue_burst|bam_fee_config_midqueue_refresh|bam_fee_config_midqueue_source_mix_queue_burst|bam_fee_config_midqueue_source_mix_multi_reconnect|source_mix_queue_burst_reconnect|disable_enable_queue_burst_reconnect|quarantine_disable_enable_queue_inflight|quarantine_url_churn_queue_inflight)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

configure_queue_burst_params() {
  local iter_mode="$1"
  local iter_seed="$2"
  ITER_QUEUE_BURST_BATCH_COUNT=32
  ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT=max
  ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS=1
  ITER_QUEUE_BURST_SECOND_CLOSE_AFTER_RESULTS=0
  case "${iter_mode}" in
    partial_drain_reconnect)
      ITER_QUEUE_BURST_BATCH_COUNT=15
      ;;
    queue_burst64_reconnect)
      ITER_QUEUE_BURST_BATCH_COUNT=64
      ;;
    bam_fee_queue_burst_reconnect|bam_fee_source_mix_queue_burst_reconnect|bam_fee_config_refresh_queue_burst|bam_fee_config_refresh_source_mix_queue_burst|bam_fee_config_midqueue_refresh|bam_fee_config_midqueue_source_mix_queue_burst|source_mix_queue_burst_reconnect|disable_enable_queue_burst_reconnect)
      ITER_QUEUE_BURST_BATCH_COUNT=64
      ;;
    quarantine_disable_enable_queue_inflight|quarantine_url_churn_queue_inflight)
      ITER_QUEUE_BURST_BATCH_COUNT=512
      ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS=0
      ;;
    queue_burst64_leader_plus1_reconnect)
      ITER_QUEUE_BURST_BATCH_COUNT=64
      ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT=leader+1
      ;;
    schedule_boundary_jitter)
      local jitter_batch_counts=(16 32 64 96 128)
      local jitter_max_schedule_slots=(leader leader+1 leader+2)
      ITER_QUEUE_BURST_BATCH_COUNT="${jitter_batch_counts[$(( iter_seed % ${#jitter_batch_counts[@]} ))]}"
      ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT="${jitter_max_schedule_slots[$(( (iter_seed / ${#jitter_batch_counts[@]}) % ${#jitter_max_schedule_slots[@]} ))]}"
      ;;
    queue_reconnect_timing_jitter)
      local timing_batch_counts=(8 16 32 64)
      local timing_max_schedule_slots=(leader leader+1 leader+2 max)
      ITER_QUEUE_BURST_BATCH_COUNT="${timing_batch_counts[$(( iter_seed % ${#timing_batch_counts[@]} ))]}"
      ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT="${timing_max_schedule_slots[$(( (iter_seed / ${#timing_batch_counts[@]}) % ${#timing_max_schedule_slots[@]} ))]}"
      case $(( (iter_seed / (${#timing_batch_counts[@]} * ${#timing_max_schedule_slots[@]})) % 5 )) in
        0) ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS=0 ;;
        1) ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS=1 ;;
        2) ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS=$(( ITER_QUEUE_BURST_BATCH_COUNT / 4 )) ;;
        3) ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS=$(( ITER_QUEUE_BURST_BATCH_COUNT / 2 )) ;;
        4) ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS=$(( ITER_QUEUE_BURST_BATCH_COUNT - 1 )) ;;
      esac
      ;;
    queue_burst_multi_reconnect|source_mix_queue_burst_multi_reconnect|bam_fee_config_midqueue_source_mix_multi_reconnect)
      ITER_QUEUE_BURST_BATCH_COUNT=64
      ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS=1
      ITER_QUEUE_BURST_SECOND_CLOSE_AFTER_RESULTS=1
      ;;
    queue_burst128_reconnect)
      ITER_QUEUE_BURST_BATCH_COUNT=128
      ;;
    queue_burst256_reconnect)
      ITER_QUEUE_BURST_BATCH_COUNT=256
      ;;
    queue_burst512_reconnect)
      ITER_QUEUE_BURST_BATCH_COUNT=512
      ;;
    queue_burst_leader_reconnect)
      ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT=leader
      ;;
    queue_burst64_leader_reconnect)
      ITER_QUEUE_BURST_BATCH_COUNT=64
      ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT=leader
      ;;
  esac
  if [[ -n "${QUEUE_BURST_BATCH_COUNT}" ]]; then
    ITER_QUEUE_BURST_BATCH_COUNT="${QUEUE_BURST_BATCH_COUNT}"
  fi
  if [[ -n "${QUEUE_BURST_MAX_SCHEDULE_SLOT}" ]]; then
    ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT="${QUEUE_BURST_MAX_SCHEDULE_SLOT}"
  fi
  if [[ -n "${QUEUE_BURST_CLOSE_AFTER_RESULTS}" ]]; then
    ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS="${QUEUE_BURST_CLOSE_AFTER_RESULTS}"
  fi
}

prepare_queue_burst_packets() {
  local iter_dir="$1"
  local input_path="$2"
  local blockhash="$3"
  local packet_one="$4"
  local packet_two="$5"
  local bridge_one="$6"
  local bridge_two="$7"
  local packet_dir="${iter_dir}/queue-packets"
  mkdir -p "${packet_dir}"
  ITER_QUEUE_PACKET_FILES=()
  local batch_idx
  for batch_idx in $(seq 0 $((ITER_QUEUE_BURST_BATCH_COUNT - 1))); do
    local seq_id=$((SEQ_ONE + batch_idx))
    local to_index=$((TO_GENESIS_ACCOUNT_ONE + batch_idx))
    local packet_file="${packet_dir}/packet_${seq_id}.txt"
    local bridge_out="${packet_dir}/bridge_${seq_id}.out"
    if (( batch_idx == 0 )); then
      packet_file="${packet_one}"
      bridge_out="${bridge_one}"
    elif (( batch_idx == 1 )); then
      packet_file="${packet_two}"
      bridge_out="${bridge_two}"
    else
      bridge_packet "${input_path}" "${packet_file}" "${bridge_out}" "${blockhash}" "${to_index}"
    fi
    ITER_QUEUE_PACKET_FILES+=( "${packet_file}" )
  done
}

prepare_fee_config_refresh_queue_packets() {
  local iter_dir="$1"
  local input_path="$2"
  local blockhash="$3"
  local packet_one="$4"
  local bridge_one="$5"
  local packet_dir="${iter_dir}/queue-packets"
  mkdir -p "${packet_dir}"
  ITER_QUEUE_PACKET_FILES=()
  local batch_idx
  for batch_idx in $(seq 0 $((ITER_QUEUE_BURST_BATCH_COUNT - 1))); do
    local seq_id=$((SEQ_TWO + batch_idx))
    local packet_file="${packet_dir}/packet_${seq_id}.txt"
    local bridge_out="${packet_dir}/bridge_${seq_id}.out"
    if (( batch_idx == 0 )); then
      packet_file="${packet_one}"
      bridge_out="${bridge_one}"
    else
      local to_index=$((TO_GENESIS_ACCOUNT_ONE + batch_idx))
      if (( TO_GENESIS_ACCOUNT_TWO >= TO_GENESIS_ACCOUNT_ONE && to_index >= TO_GENESIS_ACCOUNT_TWO )); then
        to_index=$((to_index + 1))
      fi
      bridge_packet "${input_path}" "${packet_file}" "${bridge_out}" "${blockhash}" "${to_index}"
    fi
    ITER_QUEUE_PACKET_FILES+=( "${packet_file}" )
  done
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

materialize_operator_events() {
  local source_path="$1"
  local output_path="$2"
  local iter_dir="$3"
  local alternate_identity="${iter_dir}/operator-alternate-identity.json"

  if grep -q '@ALTERNATE_IDENTITY@' "${source_path}"; then
    seeded_keypair_file 700001 "${alternate_identity}" \
      >"${iter_dir}/operator-alternate-identity.out"
  fi

  local line
  : >"${output_path}"
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line=${line//@NODE_IDENTITY@/${NODE_KEYPAIR_PATH}}
    line=${line//@ALTERNATE_IDENTITY@/${alternate_identity}}
    printf '%s\n' "${line}" >>"${output_path}"
  done <"${source_path}"

  if grep -qE '@[A-Z_]+@' "${output_path}"; then
    die "unresolved operator-event placeholder in ${output_path}"
  fi
}

prepare_valid_alt_commit_packet() {
  local iter_dir="$1"
  local blockhash="$2"
  local alt_packet_path="$3"
  local _unused_setup_packet_path="$4"
  local _unused_batch_packet_path="$5"
  local bridge_one_out="$6"
  local tx_path="${iter_dir}/valid-alt.txnctx"
  local gen_out="${iter_dir}/valid-alt-gen.out"
  local target_keypair="${iter_dir}/alt-target.json"
  local table_keypair="${iter_dir}/alt-table.json"
  local lamports="${7:-1000000}"

  local target_pubkey
  target_pubkey=$(seeded_keypair_pubkey "${TO_GENESIS_ACCOUNT_ONE}" "${target_keypair}") \
    || die "failed to derive ALT target pubkey"
  [[ -n "${target_pubkey}" ]] || die "empty ALT target pubkey"

  local lookup_table
  lookup_table=$(seeded_keypair_pubkey "${BAM_PRESEEDED_ALT_TABLE_SEED}" "${table_keypair}") \
    || die "failed to derive preseeded ALT table pubkey"
  [[ -n "${lookup_table}" ]] || die "empty preseeded ALT table pubkey"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${tx_path}" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports "${lamports}" \
    --cu-limit 300000 \
    --recent-blockhash "${blockhash}" \
    --from-seed-index "${FROM_GENESIS_ACCOUNT}" \
    --to-seed-index "${TO_GENESIS_ACCOUNT_ONE}" \
    --address-table-lookup "${lookup_table}" \
    >"${gen_out}"

  bridge_raw_packet "${tx_path}" "${alt_packet_path}" "${bridge_one_out}"

  local gen_line
  gen_line=$(grep '^wrote synthetic txnctx ' "${gen_out}" || true)
  [[ -n "${gen_line}" ]] || die "failed to capture valid-ALT generator summary"

  ITER_FROM=$(extract_gen_field from "${gen_line}")
  ITER_TO_ONE=$(extract_gen_field target "${gen_line}")
  ITER_TO_TWO=""
  ITER_SYSTEM_KIND=$(extract_gen_field system_kind "${gen_line}")
  [[ -n "${ITER_FROM}" && -n "${ITER_TO_ONE}" && "${ITER_SYSTEM_KIND}" == "transfer" ]] \
    || die "failed to parse valid-ALT generated transaction metadata"

  ITER_PAYER_INITIAL=$(rpc_balance "${ITER_FROM}" 2>/dev/null || true)
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
  ITER_INITIAL_TO_TWO=""
  ITER_EXPECT_TO_ONE=$((ITER_INITIAL_TO_ONE + lamports))
  ITER_EXPECT_TO_TWO=""
  PREPARED_INPUT_PATH="${tx_path}"
  PREPARED_INPUT_LABEL="synthetic_preseeded_valid_alt_transfer_txnctx"
  PREPARED_INPUT_NOTE="system_kind=transfer lamports=${lamports} transfer_count=1 valid_address_table_lookup=${lookup_table} lookup_target=${target_pubkey} valid_alt_setup=preseeded_dev_genesis table_seed=${BAM_PRESEEDED_ALT_TABLE_SEED} address_seed=${TO_GENESIS_ACCOUNT_ONE}"
  VALID_ALT_LOOKUP_TABLE="${lookup_table}"
}

prepare_invalid_alt_missing_table_packet() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_path="$3"
  local bridge_out="$4"
  local lamports="${5:-1000000}"
  local tx_path="${iter_dir}/invalid-alt-missing-table.txnctx"
  local gen_out="${iter_dir}/invalid-alt-missing-table-gen.out"

  generate_raw_transfer_packet \
    "${tx_path}" \
    "${packet_path}" \
    "${gen_out}" \
    "${bridge_out}" \
    "${blockhash}" \
    "${TO_GENESIS_ACCOUNT_ONE}" \
    "${lamports}" \
    1

  local gen_line
  gen_line=$(grep '^wrote synthetic txnctx ' "${gen_out}" || true)
  [[ -n "${gen_line}" ]] || die "failed to capture invalid-ALT generator summary"

  ITER_FROM=$(extract_gen_field from "${gen_line}")
  ITER_TO_ONE=$(extract_gen_field target "${gen_line}")
  ITER_TO_TWO=""
  ITER_SYSTEM_KIND=$(extract_gen_field system_kind "${gen_line}")
  [[ -n "${ITER_FROM}" && -n "${ITER_TO_ONE}" && "${ITER_SYSTEM_KIND}" == "transfer" ]] \
    || die "failed to parse invalid-ALT generated transaction metadata"

  ITER_PAYER_INITIAL=$(rpc_balance "${ITER_FROM}" 2>/dev/null || true)
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
  ITER_INITIAL_TO_TWO=""
  ITER_EXPECT_TO_ONE="${ITER_INITIAL_TO_ONE}"
  ITER_EXPECT_TO_TWO=""
  PREPARED_INPUT_PATH="${tx_path}"
  PREPARED_INPUT_LABEL="synthetic_invalid_alt_missing_table_transfer_txnctx"
  PREPARED_INPUT_NOTE="system_kind=transfer lamports=${lamports} transfer_count=1 invalid_alt_missing_table=true bogus_address_table_lookup=true bogus_lookup_seed=9999999"
}

prepare_durable_nonce_commit_packet() {
  local iter_dir="$1"
  local packet_path="$2"
  local gen_out="$3"
  local lamports="$4"
  local iter_seed="$5"
  local wrong_authority="${6:-0}"

  local payer_keypair="${iter_dir}/durable-nonce-payer.json"
  local nonce_keypair="${iter_dir}/durable-nonce-account.json"
  local nonce_seed=$((900000 + iter_seed))
  local payer_pubkey
  local nonce_pubkey
  payer_pubkey=$(seeded_keypair_pubkey "${FROM_GENESIS_ACCOUNT}" "${payer_keypair}") \
    || die "failed to write durable nonce payer keypair"
  nonce_pubkey=$(seeded_keypair_pubkey "${nonce_seed}" "${nonce_keypair}") \
    || die "failed to write durable nonce account keypair"

  local nonce_authority_keypair="${payer_keypair}"
  local nonce_authority_note="expected_nonce_authority=${payer_pubkey}"
  if [[ "${wrong_authority}" == "1" ]]; then
    local wrong_authority_keypair="${iter_dir}/durable-nonce-wrong-authority.json"
    local wrong_authority_seed=$((910000 + iter_seed))
    local wrong_authority_pubkey
    wrong_authority_pubkey=$(seeded_keypair_pubkey "${wrong_authority_seed}" "${wrong_authority_keypair}") \
      || die "failed to write durable nonce wrong-authority keypair"
    nonce_authority_keypair="${wrong_authority_keypair}"
    nonce_authority_note="wrong_authority_seed=${wrong_authority_seed} wrong_nonce_authority=${wrong_authority_pubkey} expected_nonce_authority=${payer_pubkey}"
  fi

  local nonce_hash
  if [[ "${RUNNER_KIND}" == "fullfd" ]]; then
    local nonce_hash_seed=$((BAM_PRESEEDED_NONCE_HASH_SEED_BASE + iter_seed))
    local nonce_hash_keypair="${iter_dir}/durable-nonce-hash.json"
    nonce_hash=$(seeded_keypair_pubkey "${nonce_hash_seed}" "${nonce_hash_keypair}") \
      || die "failed to derive preseeded durable nonce hash"
    wait_for_account_present "${nonce_pubkey}" "${TIMEOUT_SECS}" \
      || die "preseeded durable nonce account ${nonce_pubkey} did not appear"
    printf '%s\n' "${nonce_hash}" >"${iter_dir}/durable-nonce.hash"
    cat >"${iter_dir}/durable-nonce-create.json" <<EOF3
{"preseeded":true,"nonce_account":"${nonce_pubkey}","nonce_hash":"${nonce_hash}","nonce_seed":${nonce_seed},"nonce_hash_seed":${nonce_hash_seed}}
EOF3
  else
    wait_rpc_healthy "${TIMEOUT_SECS}" \
      || die "validator RPC did not become healthy before durable nonce setup"
    solana --url "${RPC_URL}" \
      --keypair "${payer_keypair}" \
      create-nonce-account "${nonce_keypair}" 0.01 \
      --nonce-authority "${payer_pubkey}" \
      --commitment processed \
      --output json \
      >"${iter_dir}/durable-nonce-create.json" \
      2>"${iter_dir}/durable-nonce-create.err" \
      || die "failed to create durable nonce account ${nonce_pubkey}; see ${iter_dir}/durable-nonce-create.err"

    wait_for_account_present "${nonce_pubkey}" "${TIMEOUT_SECS}" \
      || die "durable nonce account ${nonce_pubkey} did not appear"
    nonce_hash=$(wait_for_nonce_hash "${nonce_pubkey}" "${TIMEOUT_SECS}") \
      || die "durable nonce account ${nonce_pubkey} did not expose a nonce hash"
    rpc_nonce_hash "${nonce_pubkey}" >"${iter_dir}/durable-nonce.hash"
  fi

  "${GEN_DURABLE_NONCE_TRANSFER_BIN}" \
    --output "${packet_path}" \
    --nonce-hash "${nonce_hash}" \
    --nonce-keypair "${nonce_keypair}" \
    --nonce-authority-keypair "${nonce_authority_keypair}" \
    --from-keypair "${payer_keypair}" \
    --to-seed-index "${TO_GENESIS_ACCOUNT_ONE}" \
    --lamports "${lamports}" \
    --cu-limit "${CU_LIMIT}" \
    >"${gen_out}"

  local gen_line
  gen_line=$(grep '^wrote durable nonce packet ' "${gen_out}" || true)
  [[ -n "${gen_line}" ]] || die "failed to capture durable nonce generator summary"

  ITER_FROM=$(printf '%s\n' "${gen_line}" | sed -n 's/.* from=\([^ ]*\) target=.*/\1/p')
  ITER_TO_ONE=$(printf '%s\n' "${gen_line}" | sed -n 's/.* target=\([^ ]*\) nonce_account=.*/\1/p')
  ITER_TO_TWO=""
  ITER_SYSTEM_KIND="durable_nonce"
  ITER_DURABLE_NONCE_ACCOUNT="${nonce_pubkey}"
  ITER_DURABLE_NONCE_HASH="${nonce_hash}"
  ITER_PAYER_INITIAL=$(rpc_balance "${ITER_FROM}" 2>/dev/null || true)
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}" 2>/dev/null || true)
  ITER_INITIAL_TO_TWO=""
  if [[ "${wrong_authority}" == "1" ]]; then
    ITER_EXPECT_TO_ONE="${ITER_INITIAL_TO_ONE}"
  else
    ITER_EXPECT_TO_ONE=$((ITER_INITIAL_TO_ONE + lamports))
  fi
  ITER_EXPECT_TO_TWO=""
  PREPARED_INPUT_PATH="${packet_path}"
  PREPARED_INPUT_LABEL="synthetic_durable_nonce_transfer_packet"
  PREPARED_INPUT_NOTE="system_kind=durable_nonce lamports=${lamports} wrong_authority=${wrong_authority} nonce_account=${nonce_pubkey} nonce_hash=${nonce_hash} nonce_seed=${nonce_seed} ${nonce_authority_note} nonce_setup=$([[ "${RUNNER_KIND}" == "fullfd" ]] && printf 'preseeded_dev_genesis' || printf 'rpc_create_nonce_account')"
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
# Agave's status cache stores a variable 20-byte slice of a signature.  Change
# every byte so the invalid signature cannot alias the valid source signature.
for idx in range(cursor, cursor + 64):
    raw[idx] ^= 0xFF
target.write_text(base64.b64encode(raw).decode("ascii") + "\n")
PY
}

prepare_non_atomic_inconsistent_bundle_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_one_path="$3"
  local packet_two_path="$4"
  local packet_batch_path="$5"
  local bridge_one_out="$6"
  local bridge_two_out="$7"

  local reserve_lamports=1000000000
  local overflow_lamports=1000000000
  local probe_input="${iter_dir}/non-atomic-probe.txnctx"
  local probe_bridge_one_out="${iter_dir}/non-atomic-probe-recipient-1.out"
  local probe_bridge_two_out="${iter_dir}/non-atomic-probe-recipient-2.out"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${probe_input}" \
    --lamports 1 \
    >"${iter_dir}/non-atomic-probe-gen.out"

  bridge_packet "${probe_input}" "${iter_dir}/non-atomic-probe-recipient-1.txt" "${probe_bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"
  bridge_packet "${probe_input}" "${iter_dir}/non-atomic-probe-recipient-2.txt" "${probe_bridge_two_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}"

  local probe_adapt_one probe_adapt_two
  probe_adapt_one=$(grep '^adapted family=' "${probe_bridge_one_out}" || true)
  probe_adapt_two=$(grep '^adapted family=' "${probe_bridge_two_out}" || true)
  [[ -n "${probe_adapt_one}" && -n "${probe_adapt_two}" ]] || die "failed to capture non-atomic probe transfer summaries"

  ITER_FROM=$(extract_bridge_field from "${probe_adapt_one}")
  ITER_TO_ONE=$(extract_bridge_field to "${probe_adapt_one}")
  ITER_TO_TWO=$(extract_bridge_field to "${probe_adapt_two}")
  ITER_SYSTEM_KIND="transfer"

  ITER_PRE_FROM=$(rpc_balance "${ITER_FROM}")
  ITER_PAYER_INITIAL="${ITER_PRE_FROM}"
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
  ITER_INITIAL_TO_TWO=$(rpc_balance "${ITER_TO_TWO}")

  (( ITER_PRE_FROM > reserve_lamports + overflow_lamports + 1000000 ))     || die "payer balance ${ITER_PRE_FROM} is too small for the non-atomic inconsistent-bundle mode"

  local lamports_one=$(( ITER_PRE_FROM - reserve_lamports ))
  local lamports_two=$(( reserve_lamports + overflow_lamports ))

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${iter_dir}/non-atomic-tx0.txnctx" \
    --lamports "${lamports_one}" \
    >"${iter_dir}/non-atomic-tx0-gen.out"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${iter_dir}/non-atomic-tx1.txnctx" \
    --lamports "${lamports_two}" \
    >"${iter_dir}/non-atomic-tx1-gen.out"

  bridge_packet "${iter_dir}/non-atomic-tx0.txnctx" "${packet_one_path}" "${bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"
  bridge_packet "${iter_dir}/non-atomic-tx1.txnctx" "${packet_two_path}" "${bridge_two_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}"

  cat "${packet_one_path}" "${packet_two_path}" >"${packet_batch_path}"

  ITER_EXPECT_TO_ONE=$((ITER_INITIAL_TO_ONE + lamports_one))
  ITER_EXPECT_TO_TWO="${ITER_INITIAL_TO_TWO}"
  PREPARED_INPUT_PATH="${iter_dir}/non-atomic-tx0.txnctx,${iter_dir}/non-atomic-tx1.txnctx"
  PREPARED_INPUT_LABEL="synthetic_non_atomic_overdraft_pair"
  PREPARED_INPUT_NOTE="tx0_lamports=${lamports_one} tx1_lamports=${lamports_two} pre_from=${ITER_PRE_FROM} reserve_lamports=${reserve_lamports} overflow_lamports=${overflow_lamports}"
}

prepare_seq_id_wrap_conflicting_spend_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_one_path="$3"
  local packet_two_path="$4"
  local packet_batch_path="$5"
  local bridge_one_out="$6"
  local bridge_two_out="$7"

  local reserve_lamports=1000000000
  local overflow_lamports=1000000000
  local probe_input="${iter_dir}/seq-wrap-conflict-probe.txnctx"
  local probe_bridge_one_out="${iter_dir}/seq-wrap-conflict-probe-recipient-1.out"
  local probe_bridge_two_out="${iter_dir}/seq-wrap-conflict-probe-recipient-2.out"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${probe_input}" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports 1 \
    >"${iter_dir}/seq-wrap-conflict-probe-gen.out"

  bridge_packet "${probe_input}" "${iter_dir}/seq-wrap-conflict-probe-recipient-1.txt" "${probe_bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"
  bridge_packet "${probe_input}" "${iter_dir}/seq-wrap-conflict-probe-recipient-2.txt" "${probe_bridge_two_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}"

  local probe_adapt_one probe_adapt_two
  probe_adapt_one=$(grep '^adapted family=' "${probe_bridge_one_out}" || true)
  probe_adapt_two=$(grep '^adapted family=' "${probe_bridge_two_out}" || true)
  [[ -n "${probe_adapt_one}" && -n "${probe_adapt_two}" ]] || die "failed to capture seq-wrap-conflict probe transfer summaries"

  ITER_FROM=$(extract_bridge_field from "${probe_adapt_one}")
  ITER_TO_ONE=$(extract_bridge_field to "${probe_adapt_one}")
  ITER_TO_TWO=$(extract_bridge_field to "${probe_adapt_two}")
  ITER_SYSTEM_KIND="transfer"

  ITER_PRE_FROM=$(rpc_balance "${ITER_FROM}")
  ITER_PAYER_INITIAL="${ITER_PRE_FROM}"
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
  ITER_INITIAL_TO_TWO=$(rpc_balance "${ITER_TO_TWO}")

  (( ITER_PRE_FROM > reserve_lamports + overflow_lamports + 1000000 )) \
    || die "payer balance ${ITER_PRE_FROM} is too small for seq_id_wrap_conflicting_spend_multi_batch"

  local lamports_one=$(( ITER_PRE_FROM - reserve_lamports ))
  local lamports_two=$(( reserve_lamports + overflow_lamports ))

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${iter_dir}/seq-wrap-conflict-tx0.txnctx" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports "${lamports_one}" \
    >"${iter_dir}/seq-wrap-conflict-tx0-gen.out"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${iter_dir}/seq-wrap-conflict-tx1.txnctx" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports "${lamports_two}" \
    >"${iter_dir}/seq-wrap-conflict-tx1-gen.out"

  bridge_packet "${iter_dir}/seq-wrap-conflict-tx0.txnctx" "${packet_one_path}" "${bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"
  bridge_packet "${iter_dir}/seq-wrap-conflict-tx1.txnctx" "${packet_two_path}" "${bridge_two_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}"
  cat "${packet_one_path}" "${packet_two_path}" >"${packet_batch_path}"

  ITER_OWNER_ONE=""
  ITER_SPACE_ONE=""
  ITER_OWNER_TWO=""
  ITER_SPACE_TWO=""
  ITER_EXPECT_TO_ONE=$((ITER_INITIAL_TO_ONE + lamports_one))
  ITER_EXPECT_TO_TWO="${ITER_INITIAL_TO_TWO}"
  PREPARED_INPUT_PATH="${iter_dir}/seq-wrap-conflict-tx0.txnctx,${iter_dir}/seq-wrap-conflict-tx1.txnctx"
  PREPARED_INPUT_LABEL="synthetic_seq_wrap_conflicting_spend_pair"
  PREPARED_INPUT_NOTE="tx0_lamports=${lamports_one} tx1_lamports=${lamports_two} pre_from=${ITER_PRE_FROM} reserve_lamports=${reserve_lamports} overflow_lamports=${overflow_lamports} fd_expected_winner=seq_id_${SEQ_TWO:-4294967295}"
}


prepare_duplicate_seq_split_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_batch_path="$3"
  local base_lamports="$4"

  : >"${packet_batch_path}"

  local idx first_adapt="" total_lamports=0 input_paths=""
  for idx in 0 1 2 3 4 5; do
    local lamports_i=$((base_lamports + idx))
    local input_path="${iter_dir}/input_${idx}.txnctx"
    local gen_out="${iter_dir}/gen_${idx}.out"
    local packet_path="${iter_dir}/packet_${idx}.txt"
    local bridge_out="${iter_dir}/bridge_${idx}.out"

    "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
      --output "${input_path}" \
      --system-kind transfer \
      --transfer-count 1 \
      --lamports "${lamports_i}" \
      --cu-limit "${CU_LIMIT}" \
      >"${gen_out}"

    bridge_packet "${input_path}" "${packet_path}" "${bridge_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"
    cat "${packet_path}" >>"${packet_batch_path}"
    if [[ -z "${first_adapt}" ]]; then
      first_adapt=$(grep '^adapted family=' "${bridge_out}" || true)
    fi
    total_lamports=$((total_lamports + lamports_i))
    if [[ -z "${input_paths}" ]]; then
      input_paths="${input_path}"
    else
      input_paths="${input_paths},${input_path}"
    fi
  done

  [[ -n "${first_adapt}" ]] || die "failed to capture duplicate_seq_split adapted system summary"
  ITER_SYSTEM_KIND=$(extract_bridge_field system_kind "${first_adapt}")
  ITER_FROM=$(extract_bridge_field from "${first_adapt}")
  ITER_TO_ONE=$(extract_bridge_field to "${first_adapt}")
  [[ -n "${ITER_SYSTEM_KIND}" && -n "${ITER_FROM}" && -n "${ITER_TO_ONE}" ]] \
    || die "failed to parse duplicate_seq_split adapted system transaction"

  ITER_TO_TWO=""
  ITER_OWNER_ONE=""
  ITER_SPACE_ONE=""
  ITER_OWNER_TWO=""
  ITER_SPACE_TWO=""
  ITER_PAYER_INITIAL=$(rpc_balance "${ITER_FROM}" 2>/dev/null || true)
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
  ITER_INITIAL_TO_TWO=""
  ITER_EXPECT_TO_ONE=$((ITER_INITIAL_TO_ONE + total_lamports))
  ITER_EXPECT_TO_TWO=""
  PREPARED_INPUT_PATH="${input_paths}"
  PREPARED_INPUT_LABEL="synthetic_duplicate_seq_split_transfer_txnctx"
  PREPARED_INPUT_NOTE="base_lamports=${base_lamports} transfer_count=6 total_lamports=${total_lamports} seq_id=${SEQ_ONE}"
}

prepare_atomic_revert_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_one_path="$3"
  local packet_two_path="$4"
  local packet_batch_path="$5"
  local bridge_one_out="$6"
  local bridge_two_out="$7"

  local reserve_lamports=1000000000
  local overflow_lamports=1000000000
  local probe_input="${iter_dir}/atomic-revert-probe.txnctx"
  local probe_bridge_one_out="${iter_dir}/atomic-revert-probe-recipient-1.out"
  local probe_bridge_two_out="${iter_dir}/atomic-revert-probe-recipient-2.out"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${probe_input}" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports 1 \
    >"${iter_dir}/atomic-revert-probe-gen.out"

  bridge_packet "${probe_input}" "${iter_dir}/atomic-revert-probe-recipient-1.txt" "${probe_bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"
  bridge_packet "${probe_input}" "${iter_dir}/atomic-revert-probe-recipient-2.txt" "${probe_bridge_two_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}"

  local probe_adapt_one probe_adapt_two
  probe_adapt_one=$(grep '^adapted family=' "${probe_bridge_one_out}" || true)
  probe_adapt_two=$(grep '^adapted family=' "${probe_bridge_two_out}" || true)
  [[ -n "${probe_adapt_one}" && -n "${probe_adapt_two}" ]] || die "failed to capture atomic-revert probe transfer summaries"

  ITER_FROM=$(extract_bridge_field from "${probe_adapt_one}")
  ITER_TO_ONE=$(extract_bridge_field to "${probe_adapt_one}")
  ITER_TO_TWO=$(extract_bridge_field to "${probe_adapt_two}")
  ITER_SYSTEM_KIND="transfer"

  ITER_PRE_FROM=$(rpc_balance "${ITER_FROM}")
  ITER_PAYER_INITIAL="${ITER_PRE_FROM}"
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
  ITER_INITIAL_TO_TWO=$(rpc_balance "${ITER_TO_TWO}")

  (( ITER_PRE_FROM > reserve_lamports + overflow_lamports + 1000000 )) \
    || die "payer balance ${ITER_PRE_FROM} is too small for atomic_revert"

  local lamports_one=$(( ITER_PRE_FROM - reserve_lamports ))
  local lamports_two=$(( reserve_lamports + overflow_lamports ))

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${iter_dir}/tx0.txnctx" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports "${lamports_one}" \
    >"${iter_dir}/tx0-gen.out"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${iter_dir}/tx1.txnctx" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports "${lamports_two}" \
    >"${iter_dir}/tx1-gen.out"

  bridge_packet "${iter_dir}/tx0.txnctx" "${packet_one_path}" "${bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"
  bridge_packet "${iter_dir}/tx1.txnctx" "${packet_two_path}" "${bridge_two_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}"

  cat "${packet_one_path}" "${packet_two_path}" >"${packet_batch_path}"

  ITER_OWNER_ONE=""
  ITER_SPACE_ONE=""
  ITER_OWNER_TWO=""
  ITER_SPACE_TWO=""
  ITER_EXPECT_TO_ONE="${ITER_INITIAL_TO_ONE}"
  ITER_EXPECT_TO_TWO="${ITER_INITIAL_TO_TWO}"
  PREPARED_INPUT_PATH="${iter_dir}/tx0.txnctx,${iter_dir}/tx1.txnctx"
  PREPARED_INPUT_LABEL="synthetic_atomic_revert_overdraft_pair"
  PREPARED_INPUT_NOTE="tx0_lamports=${lamports_one} tx1_lamports=${lamports_two} pre_from=${ITER_PRE_FROM} reserve_lamports=${reserve_lamports} overflow_lamports=${overflow_lamports} failing_index=1"
}

prepare_atomic_first_overdraft_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_one_path="$3"
  local packet_two_path="$4"
  local packet_batch_path="$5"
  local bridge_one_out="$6"
  local bridge_two_out="$7"

  local overflow_lamports=1000000000
  local suffix_lamports=1000000
  local probe_input="${iter_dir}/atomic-first-probe.txnctx"
  local probe_bridge_one_out="${iter_dir}/atomic-first-probe-recipient-1.out"
  local probe_bridge_two_out="${iter_dir}/atomic-first-probe-recipient-2.out"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${probe_input}" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports 1 \
    >"${iter_dir}/atomic-first-probe-gen.out"

  bridge_packet "${probe_input}" "${iter_dir}/atomic-first-probe-recipient-1.txt" "${probe_bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"
  bridge_packet "${probe_input}" "${iter_dir}/atomic-first-probe-recipient-2.txt" "${probe_bridge_two_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}"

  local probe_adapt_one probe_adapt_two
  probe_adapt_one=$(grep '^adapted family=' "${probe_bridge_one_out}" || true)
  probe_adapt_two=$(grep '^adapted family=' "${probe_bridge_two_out}" || true)
  [[ -n "${probe_adapt_one}" && -n "${probe_adapt_two}" ]] || die "failed to capture atomic first-overdraft probe summaries"

  ITER_FROM=$(extract_bridge_field from "${probe_adapt_one}")
  ITER_TO_ONE=$(extract_bridge_field to "${probe_adapt_one}")
  ITER_TO_TWO=$(extract_bridge_field to "${probe_adapt_two}")
  ITER_SYSTEM_KIND="transfer"

  ITER_PRE_FROM=$(rpc_balance "${ITER_FROM}")
  ITER_PAYER_INITIAL="${ITER_PRE_FROM}"
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
  ITER_INITIAL_TO_TWO=$(rpc_balance "${ITER_TO_TWO}")

  (( ITER_PRE_FROM > suffix_lamports + 10000000 )) \
    || die "payer balance ${ITER_PRE_FROM} is too small for the atomic first-overdraft mode"

  local lamports_one=$(( ITER_PRE_FROM + overflow_lamports ))
  local lamports_two=${suffix_lamports}

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${iter_dir}/tx0.txnctx" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports "${lamports_one}" \
    >"${iter_dir}/tx0-gen.out"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${iter_dir}/tx1.txnctx" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports "${lamports_two}" \
    >"${iter_dir}/tx1-gen.out"

  bridge_packet "${iter_dir}/tx0.txnctx" "${packet_one_path}" "${bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"
  bridge_packet "${iter_dir}/tx1.txnctx" "${packet_two_path}" "${bridge_two_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}"

  cat "${packet_one_path}" "${packet_two_path}" >"${packet_batch_path}"

  ITER_OWNER_ONE=""
  ITER_SPACE_ONE=""
  ITER_OWNER_TWO=""
  ITER_SPACE_TWO=""
  ITER_EXPECT_TO_ONE="${ITER_INITIAL_TO_ONE}"
  ITER_EXPECT_TO_TWO="${ITER_INITIAL_TO_TWO}"
  PREPARED_INPUT_PATH="${iter_dir}/tx0.txnctx,${iter_dir}/tx1.txnctx"
  PREPARED_INPUT_LABEL="synthetic_atomic_first_overdraft_pair"
  PREPARED_INPUT_NOTE="tx0_lamports=${lamports_one} tx1_lamports=${lamports_two} pre_from=${ITER_PRE_FROM} overflow_lamports=${overflow_lamports} failing_index=0 suffix_must_not_execute=true"
}

prepare_atomic_mid_fail_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_one_path="$3"
  local packet_two_path="$4"
  local packet_batch_path="$5"
  local bridge_one_out="$6"
  local bridge_two_out="$7"

  local packet_three_path="${iter_dir}/packet_three.txt"
  local bridge_three_out="${iter_dir}/bridge_three.out"
  local probe_input="${iter_dir}/atomic-mid-probe.txnctx"
  local probe_bridge_one_out="${iter_dir}/atomic-mid-probe-recipient-1.out"
  local probe_bridge_two_out="${iter_dir}/atomic-mid-probe-recipient-2.out"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${probe_input}" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports 1 \
    >"${iter_dir}/atomic-mid-probe-gen.out"

  bridge_packet "${probe_input}" "${iter_dir}/atomic-mid-probe-recipient-1.txt" "${probe_bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"
  bridge_packet "${probe_input}" "${iter_dir}/atomic-mid-probe-recipient-2.txt" "${probe_bridge_two_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}"

  local probe_adapt_one probe_adapt_two
  probe_adapt_one=$(grep '^adapted family=' "${probe_bridge_one_out}" || true)
  probe_adapt_two=$(grep '^adapted family=' "${probe_bridge_two_out}" || true)
  [[ -n "${probe_adapt_one}" && -n "${probe_adapt_two}" ]] || die "failed to capture atomic-mid-fail probe transfer summaries"

  ITER_FROM=$(extract_bridge_field from "${probe_adapt_one}")
  ITER_TO_ONE=$(extract_bridge_field to "${probe_adapt_one}")
  ITER_TO_TWO=$(extract_bridge_field to "${probe_adapt_two}")

  ITER_PRE_FROM=$(rpc_balance "${ITER_FROM}")
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
  ITER_INITIAL_TO_TWO=$(rpc_balance "${ITER_TO_TWO}")

  local first_lamports=1000000
  local third_lamports=1000001
  (( ITER_PRE_FROM > first_lamports + third_lamports + 10000000 )) \
    || die "payer balance ${ITER_PRE_FROM} is too small for atomic_mid_fail"
  local failing_lamports="${ITER_PRE_FROM}"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${iter_dir}/atomic-mid-tx0.txnctx" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports "${first_lamports}" \
    >"${iter_dir}/atomic-mid-tx0-gen.out"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${iter_dir}/atomic-mid-tx1.txnctx" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports "${failing_lamports}" \
    >"${iter_dir}/atomic-mid-tx1-gen.out"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${iter_dir}/atomic-mid-tx2.txnctx" \
    --system-kind transfer \
    --transfer-count 1 \
    --lamports "${third_lamports}" \
    >"${iter_dir}/atomic-mid-tx2-gen.out"

  bridge_packet "${iter_dir}/atomic-mid-tx0.txnctx" "${packet_one_path}"   "${bridge_one_out}"   "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"
  bridge_packet "${iter_dir}/atomic-mid-tx1.txnctx" "${packet_two_path}"   "${bridge_two_out}"   "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}"
  bridge_packet "${iter_dir}/atomic-mid-tx2.txnctx" "${packet_three_path}" "${bridge_three_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"

  cat "${packet_one_path}" "${packet_two_path}" "${packet_three_path}" >"${packet_batch_path}"

  ITER_SYSTEM_KIND="transfer"
  ITER_EXPECT_TO_ONE="${ITER_INITIAL_TO_ONE}"
  ITER_EXPECT_TO_TWO="${ITER_INITIAL_TO_TWO}"
  PREPARED_INPUT_PATH="${iter_dir}/atomic-mid-tx0.txnctx,${iter_dir}/atomic-mid-tx1.txnctx,${iter_dir}/atomic-mid-tx2.txnctx"
  PREPARED_INPUT_LABEL="synthetic_atomic_mid_fail_transfer_triplet"
  PREPARED_INPUT_NOTE="tx0_lamports=${first_lamports} tx1_lamports=${failing_lamports} tx2_lamports=${third_lamports} pre_from=${ITER_PRE_FROM} failing_index=1"
}

prepare_atomic_duplicate_sig_mid_fail_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_one_path="$3"
  local packet_two_path="$4"
  local packet_batch_path="$5"
  local bridge_one_out="$6"
  local bridge_two_out="$7"

  local packet_three_path="${iter_dir}/packet_three.txt"
  local bridge_three_out="${iter_dir}/bridge_three.out"
  local tx0="${iter_dir}/atomic-duplicate-sig-tx0.txnctx"
  local tx1="${iter_dir}/atomic-duplicate-sig-tx1-copy.txnctx"
  local tx2="${iter_dir}/atomic-duplicate-sig-tx2.txnctx"
  local gen0="${iter_dir}/atomic-duplicate-sig-tx0-gen.out"
  local gen1="${iter_dir}/atomic-duplicate-sig-tx1-gen.out"
  local gen2="${iter_dir}/atomic-duplicate-sig-tx2-gen.out"
  local first_lamports=1000000
  local third_lamports=1000001

  generate_raw_transfer_packet "${tx0}" "${packet_one_path}"   "${gen0}" "${bridge_one_out}"   "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}" "${first_lamports}" 0
  cp "${tx0}" "${tx1}"
  cp "${packet_one_path}" "${packet_two_path}"
  cp "${gen0}" "${gen1}"
  cp "${bridge_one_out}" "${bridge_two_out}"
  generate_raw_transfer_packet "${tx2}" "${packet_three_path}" "${gen2}" "${bridge_three_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}" "${third_lamports}" 0

  cat "${packet_one_path}" "${packet_two_path}" "${packet_three_path}" >"${packet_batch_path}"

  local gen_line_one gen_line_three
  gen_line_one=$(grep '^wrote synthetic txnctx ' "${gen0}" || true)
  gen_line_three=$(grep '^wrote synthetic txnctx ' "${gen2}" || true)
  [[ -n "${gen_line_one}" && -n "${gen_line_three}" ]] \
    || die "failed to capture atomic-duplicate-signature generator summaries"

  ITER_FROM=$(extract_gen_field from "${gen_line_one}")
  ITER_TO_ONE=$(extract_gen_field target "${gen_line_one}")
  ITER_TO_TWO=$(extract_gen_field target "${gen_line_three}")
  ITER_SYSTEM_KIND=$(extract_gen_field system_kind "${gen_line_one}")
  [[ -n "${ITER_FROM}" && -n "${ITER_TO_ONE}" && -n "${ITER_TO_TWO}" && "${ITER_SYSTEM_KIND}" == "transfer" ]] \
    || die "failed to parse atomic-duplicate-signature generated transaction metadata"

  ITER_PRE_FROM=$(rpc_balance "${ITER_FROM}")
  ITER_PAYER_INITIAL="${ITER_PRE_FROM}"
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
  ITER_INITIAL_TO_TWO=$(rpc_balance "${ITER_TO_TWO}")
  ITER_OWNER_ONE=""
  ITER_SPACE_ONE=""
  ITER_OWNER_TWO=""
  ITER_SPACE_TWO=""
  ITER_EXPECT_TO_ONE="${ITER_INITIAL_TO_ONE}"
  ITER_EXPECT_TO_TWO="${ITER_INITIAL_TO_TWO}"
  PREPARED_INPUT_PATH="${tx0},${tx1},${tx2}"
  PREPARED_INPUT_LABEL="synthetic_atomic_duplicate_signature_triplet"
  PREPARED_INPUT_NOTE="tx0_lamports=${first_lamports} tx1_lamports=${first_lamports} tx2_lamports=${third_lamports} duplicate_signature=true failing_index=1 suffix_must_not_execute=true"
}

prepare_atomic_blockhash_mid_fail_packets() {
  local iter_dir="$1"
  local old_blockhash="$2"
  local fresh_blockhash="$3"
  local packet_one_path="$4"
  local packet_two_path="$5"
  local packet_batch_path="$6"
  local bridge_one_out="$7"
  local bridge_two_out="$8"
  local old_slot="$9"
  local waited_slot="${10}"

  local packet_three_path="${iter_dir}/packet_three.txt"
  local bridge_three_out="${iter_dir}/bridge_three.out"
  local tx0="${iter_dir}/atomic-blockhash-tx0-fresh.txnctx"
  local tx1="${iter_dir}/atomic-blockhash-tx1-expired.txnctx"
  local tx2="${iter_dir}/atomic-blockhash-tx2-fresh.txnctx"
  local gen0="${iter_dir}/atomic-blockhash-tx0-gen.out"
  local gen1="${iter_dir}/atomic-blockhash-tx1-gen.out"
  local gen2="${iter_dir}/atomic-blockhash-tx2-gen.out"
  local first_lamports=1000000
  local second_lamports=1000001
  local third_lamports=1000002

  generate_raw_transfer_packet "${tx0}" "${packet_one_path}"   "${gen0}" "${bridge_one_out}"   "${fresh_blockhash}" "${TO_GENESIS_ACCOUNT_ONE}" "${first_lamports}" 0
  generate_raw_transfer_packet "${tx1}" "${packet_two_path}"   "${gen1}" "${bridge_two_out}"   "${old_blockhash}"   "${TO_GENESIS_ACCOUNT_TWO}" "${second_lamports}" 0
  generate_raw_transfer_packet "${tx2}" "${packet_three_path}" "${gen2}" "${bridge_three_out}" "${fresh_blockhash}" "${TO_GENESIS_ACCOUNT_ONE}" "${third_lamports}" 0

  cat "${packet_one_path}" "${packet_two_path}" "${packet_three_path}" >"${packet_batch_path}"

  local gen_line_one gen_line_two
  gen_line_one=$(grep '^wrote synthetic txnctx ' "${gen0}" || true)
  gen_line_two=$(grep '^wrote synthetic txnctx ' "${gen1}" || true)
  [[ -n "${gen_line_one}" && -n "${gen_line_two}" ]] \
    || die "failed to capture atomic-blockhash generator summaries"

  ITER_FROM=$(extract_gen_field from "${gen_line_one}")
  ITER_TO_ONE=$(extract_gen_field target "${gen_line_one}")
  ITER_TO_TWO=$(extract_gen_field target "${gen_line_two}")
  ITER_SYSTEM_KIND=$(extract_gen_field system_kind "${gen_line_one}")
  [[ -n "${ITER_FROM}" && -n "${ITER_TO_ONE}" && -n "${ITER_TO_TWO}" && "${ITER_SYSTEM_KIND}" == "transfer" ]] \
    || die "failed to parse atomic-blockhash generated transaction metadata"

  ITER_PRE_FROM=$(rpc_balance "${ITER_FROM}")
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
  ITER_INITIAL_TO_TWO=$(rpc_balance "${ITER_TO_TWO}")
  ITER_EXPECT_TO_ONE="${ITER_INITIAL_TO_ONE}"
  ITER_EXPECT_TO_TWO="${ITER_INITIAL_TO_TWO}"
  PREPARED_INPUT_PATH="${tx0},${tx1},${tx2}"
  PREPARED_INPUT_LABEL="synthetic_atomic_blockhash_mid_fail_transfer_triplet"
  PREPARED_INPUT_NOTE="tx0_lamports=${first_lamports} tx1_lamports=${second_lamports} tx2_lamports=${third_lamports} old_blockhash=${old_blockhash} fresh_blockhash=${fresh_blockhash} old_slot=${old_slot} waited_slot=${waited_slot} failing_index=1"
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
  local cu_limit="${9:-300000}"

  local -a gen_args=(
    "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}"
    --output "${output_txnctx}"
    --system-kind transfer
    --transfer-count 1
    --lamports "${lamports}"
    --cu-limit "${cu_limit}"
    --recent-blockhash "${blockhash}"
    --from-seed-index "${FROM_GENESIS_ACCOUNT}"
    --to-seed-index "${to_seed_index}"
  )
  if [[ "${bogus_lookup}" == "1" ]]; then
    gen_args+=(--bogus-address-table-lookup)
  fi

  "${gen_args[@]}" >"${gen_out}"
  bridge_raw_packet "${output_txnctx}" "${output_packet}" "${bridge_out}"
}

set_non_atomic_partial_pair_metadata() {
  local tx0="$1"
  local tx1="$2"
  local gen0="$3"
  local gen1="$4"
  local label="$5"
  local note="$6"

  local gen_line_one gen_line_two
  gen_line_one=$(grep '^wrote synthetic txnctx ' "${gen0}" || true)
  gen_line_two=$(grep '^wrote synthetic txnctx ' "${gen1}" || true)
  [[ -n "${gen_line_one}" && -n "${gen_line_two}" ]] \
    || die "failed to capture non-atomic partial generator summaries"

  local first_lamports
  ITER_FROM=$(extract_gen_field from "${gen_line_one}")
  ITER_TO_ONE=$(extract_gen_field target "${gen_line_one}")
  ITER_TO_TWO=$(extract_gen_field target "${gen_line_two}")
  ITER_SYSTEM_KIND=$(extract_gen_field system_kind "${gen_line_one}")
  first_lamports=$(extract_gen_field total_lamports "${gen_line_one}")
  [[ -n "${ITER_FROM}" && -n "${ITER_TO_ONE}" && -n "${ITER_TO_TWO}" && "${first_lamports}" =~ ^[0-9]+$ && "${ITER_SYSTEM_KIND}" == "transfer" ]] \
    || die "failed to parse non-atomic partial generated transaction metadata"

  ITER_PRE_FROM=$(rpc_balance "${ITER_FROM}")
  ITER_PAYER_INITIAL="${ITER_PRE_FROM}"
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
  ITER_INITIAL_TO_TWO=$(rpc_balance "${ITER_TO_TWO}")
  ITER_EXPECT_TO_ONE=$((ITER_INITIAL_TO_ONE + first_lamports))
  ITER_EXPECT_TO_TWO="${ITER_INITIAL_TO_TWO}"
  if [[ "${ITER_TO_ONE}" == "${ITER_TO_TWO}" ]]; then
    ITER_EXPECT_TO_TWO=""
  fi
  PREPARED_INPUT_PATH="${tx0},${tx1}"
  PREPARED_INPUT_LABEL="${label}"
  PREPARED_INPUT_NOTE="${note} pre_from=${ITER_PRE_FROM} failing_index=1"
}

prepare_non_atomic_first_overdraft_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_one_path="$3"
  local packet_two_path="$4"
  local packet_batch_path="$5"
  local bridge_one_out="$6"
  local bridge_two_out="$7"

  local overflow_lamports=1000000000
  local suffix_lamports=1000000
  local probe_input="${iter_dir}/non-atomic-first-probe.txnctx"
  local probe_bridge_one_out="${iter_dir}/non-atomic-first-probe-recipient-1.out"
  local probe_bridge_two_out="${iter_dir}/non-atomic-first-probe-recipient-2.out"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${probe_input}" \
    --lamports 1 \
    >"${iter_dir}/non-atomic-first-probe-gen.out"

  bridge_packet "${probe_input}" "${iter_dir}/non-atomic-first-probe-recipient-1.txt" "${probe_bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"
  bridge_packet "${probe_input}" "${iter_dir}/non-atomic-first-probe-recipient-2.txt" "${probe_bridge_two_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}"

  local probe_adapt_one probe_adapt_two
  probe_adapt_one=$(grep '^adapted family=' "${probe_bridge_one_out}" || true)
  probe_adapt_two=$(grep '^adapted family=' "${probe_bridge_two_out}" || true)
  [[ -n "${probe_adapt_one}" && -n "${probe_adapt_two}" ]] || die "failed to capture non-atomic first-overdraft probe summaries"

  ITER_FROM=$(extract_bridge_field from "${probe_adapt_one}")
  ITER_TO_ONE=$(extract_bridge_field to "${probe_adapt_one}")
  ITER_TO_TWO=$(extract_bridge_field to "${probe_adapt_two}")
  ITER_SYSTEM_KIND="transfer"

  ITER_PRE_FROM=$(rpc_balance "${ITER_FROM}")
  ITER_PAYER_INITIAL="${ITER_PRE_FROM}"
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
  ITER_INITIAL_TO_TWO=$(rpc_balance "${ITER_TO_TWO}")

  (( ITER_PRE_FROM > suffix_lamports + 10000000 )) \
    || die "payer balance ${ITER_PRE_FROM} is too small for the non-atomic first-overdraft mode"

  local lamports_one=$(( ITER_PRE_FROM + overflow_lamports ))
  local lamports_two=${suffix_lamports}

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${iter_dir}/non-atomic-first-tx0.txnctx" \
    --lamports "${lamports_one}" \
    >"${iter_dir}/non-atomic-first-tx0-gen.out"

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${iter_dir}/non-atomic-first-tx1.txnctx" \
    --lamports "${lamports_two}" \
    >"${iter_dir}/non-atomic-first-tx1-gen.out"

  bridge_packet "${iter_dir}/non-atomic-first-tx0.txnctx" "${packet_one_path}" "${bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}"
  bridge_packet "${iter_dir}/non-atomic-first-tx1.txnctx" "${packet_two_path}" "${bridge_two_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}"

  cat "${packet_one_path}" "${packet_two_path}" >"${packet_batch_path}"

  ITER_EXPECT_TO_ONE="${ITER_INITIAL_TO_ONE}"
  ITER_EXPECT_TO_TWO=$((ITER_INITIAL_TO_TWO + lamports_two))
  PREPARED_INPUT_PATH="${iter_dir}/non-atomic-first-tx0.txnctx,${iter_dir}/non-atomic-first-tx1.txnctx"
  PREPARED_INPUT_LABEL="synthetic_non_atomic_first_overdraft_pair"
  PREPARED_INPUT_NOTE="tx0_lamports=${lamports_one} tx1_lamports=${lamports_two} pre_from=${ITER_PRE_FROM} overflow_lamports=${overflow_lamports} failing_index=0 suffix_should_commit=true"
}

prepare_non_atomic_partial_overdraft_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_one_path="$3"
  local packet_two_path="$4"
  local packet_batch_path="$5"
  local bridge_one_out="$6"
  local bridge_two_out="$7"

  prepare_non_atomic_inconsistent_bundle_packets \
    "${iter_dir}" \
    "${blockhash}" \
    "${packet_one_path}" \
    "${packet_two_path}" \
    "${packet_batch_path}" \
    "${bridge_one_out}" \
    "${bridge_two_out}"

  PREPARED_INPUT_LABEL="synthetic_non_atomic_partial_overdraft_pair"
  PREPARED_INPUT_NOTE="${PREPARED_INPUT_NOTE} non_atomic_partial_overdraft=true failing_index=1"
}

prepare_non_atomic_mid_overdraft_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_one_path="$3"
  local packet_two_path="$4"
  local packet_batch_path="$5"
  local bridge_one_out="$6"
  local bridge_two_out="$7"

  prepare_atomic_mid_fail_packets \
    "${iter_dir}" \
    "${blockhash}" \
    "${packet_one_path}" \
    "${packet_two_path}" \
    "${packet_batch_path}" \
    "${bridge_one_out}" \
    "${bridge_two_out}"

  ITER_EXPECT_TO_ONE=$((ITER_INITIAL_TO_ONE + 2000001))
  PREPARED_INPUT_LABEL="synthetic_non_atomic_mid_overdraft_triplet"
  PREPARED_INPUT_NOTE="${PREPARED_INPUT_NOTE} non_atomic_mid_overdraft=true revert_on_error=false"
}

prepare_non_atomic_partial_resolver_fail_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_one_path="$3"
  local packet_two_path="$4"
  local packet_batch_path="$5"
  local bridge_one_out="$6"
  local bridge_two_out="$7"

  local tx0="${iter_dir}/non-atomic-resolver-tx0.txnctx"
  local tx1="${iter_dir}/non-atomic-resolver-tx1-bogus-lookup.txnctx"
  local gen0="${iter_dir}/non-atomic-resolver-tx0-gen.out"
  local gen1="${iter_dir}/non-atomic-resolver-tx1-gen.out"
  local first_lamports=1000000
  local second_lamports=1000001

  generate_raw_transfer_packet "${tx0}" "${packet_one_path}" "${gen0}" "${bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}" "${first_lamports}" 0
  generate_raw_transfer_packet "${tx1}" "${packet_two_path}" "${gen1}" "${bridge_two_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}" "${second_lamports}" 1
  cat "${packet_one_path}" "${packet_two_path}" >"${packet_batch_path}"

  set_non_atomic_partial_pair_metadata \
    "${tx0}" \
    "${tx1}" \
    "${gen0}" \
    "${gen1}" \
    "synthetic_non_atomic_partial_resolver_fail_pair" \
    "tx0_lamports=${first_lamports} tx1_lamports=${second_lamports} bogus_address_table_lookup=true"
}

prepare_non_atomic_partial_blockhash_fail_packets() {
  local iter_dir="$1"
  local old_blockhash="$2"
  local fresh_blockhash="$3"
  local packet_one_path="$4"
  local packet_two_path="$5"
  local packet_batch_path="$6"
  local bridge_one_out="$7"
  local bridge_two_out="$8"
  local old_slot="$9"
  local waited_slot="${10}"

  local tx0="${iter_dir}/non-atomic-blockhash-tx0-fresh.txnctx"
  local tx1="${iter_dir}/non-atomic-blockhash-tx1-expired.txnctx"
  local gen0="${iter_dir}/non-atomic-blockhash-tx0-gen.out"
  local gen1="${iter_dir}/non-atomic-blockhash-tx1-gen.out"
  local first_lamports=1000000
  local second_lamports=1000001

  generate_raw_transfer_packet "${tx0}" "${packet_one_path}" "${gen0}" "${bridge_one_out}" "${fresh_blockhash}" "${TO_GENESIS_ACCOUNT_ONE}" "${first_lamports}" 0
  generate_raw_transfer_packet "${tx1}" "${packet_two_path}" "${gen1}" "${bridge_two_out}" "${old_blockhash}" "${TO_GENESIS_ACCOUNT_TWO}" "${second_lamports}" 0
  cat "${packet_one_path}" "${packet_two_path}" >"${packet_batch_path}"

  set_non_atomic_partial_pair_metadata \
    "${tx0}" \
    "${tx1}" \
    "${gen0}" \
    "${gen1}" \
    "synthetic_non_atomic_partial_blockhash_fail_pair" \
    "tx0_lamports=${first_lamports} tx1_lamports=${second_lamports} old_blockhash=${old_blockhash} fresh_blockhash=${fresh_blockhash} old_slot=${old_slot} waited_slot=${waited_slot}"

  # Full Firedancer validates the assembled bundle's minimum blockhash slot in
  # pack before scheduling either independent member. Frankendancer can execute
  # the first member before resolh rejects the expired second member.
  if [[ "${RUNNER_KIND}" == "fullfd" ]]; then
    ITER_EXPECT_TO_ONE="${ITER_INITIAL_TO_ONE}"
  fi
}

prepare_non_atomic_partial_duplicate_sig_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_one_path="$3"
  local packet_two_path="$4"
  local packet_batch_path="$5"
  local bridge_one_out="$6"
  local bridge_two_out="$7"

  local tx0="${iter_dir}/non-atomic-duplicate-sig-tx0.txnctx"
  local tx1="${iter_dir}/non-atomic-duplicate-sig-tx1-copy.txnctx"
  local gen0="${iter_dir}/non-atomic-duplicate-sig-tx0-gen.out"
  local gen1="${iter_dir}/non-atomic-duplicate-sig-tx1-gen.out"
  local first_lamports=1000000

  generate_raw_transfer_packet "${tx0}" "${packet_one_path}" "${gen0}" "${bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}" "${first_lamports}" 0
  cp "${packet_one_path}" "${packet_two_path}"
  cp "${tx0}" "${tx1}"
  cp "${gen0}" "${gen1}"
  cp "${bridge_one_out}" "${bridge_two_out}"
  cat "${packet_one_path}" "${packet_two_path}" >"${packet_batch_path}"

  set_non_atomic_partial_pair_metadata \
    "${tx0}" \
    "${tx1}" \
    "${gen0}" \
    "${gen1}" \
    "synthetic_non_atomic_partial_duplicate_signature_pair" \
    "tx0_lamports=${first_lamports} duplicate_signature=true"
}

prepare_non_atomic_partial_cu_fail_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_one_path="$3"
  local packet_two_path="$4"
  local packet_batch_path="$5"
  local bridge_one_out="$6"
  local bridge_two_out="$7"

  local tx0="${iter_dir}/non-atomic-cu-tx0.txnctx"
  local tx1="${iter_dir}/non-atomic-cu-tx1-low-cu.txnctx"
  local gen0="${iter_dir}/non-atomic-cu-tx0-gen.out"
  local gen1="${iter_dir}/non-atomic-cu-tx1-gen.out"
  local first_lamports=1000000
  local second_lamports=1000001

  generate_raw_transfer_packet "${tx0}" "${packet_one_path}" "${gen0}" "${bridge_one_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}" "${first_lamports}" 0 300000
  generate_raw_transfer_packet "${tx1}" "${packet_two_path}" "${gen1}" "${bridge_two_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}" "${second_lamports}" 0 1
  cat "${packet_one_path}" "${packet_two_path}" >"${packet_batch_path}"

  set_non_atomic_partial_pair_metadata \
    "${tx0}" \
    "${tx1}" \
    "${gen0}" \
    "${gen1}" \
    "synthetic_non_atomic_partial_cu_fail_pair" \
    "tx0_lamports=${first_lamports} tx1_lamports=${second_lamports} tx0_cu_limit=300000 tx1_cu_limit=1"
}

prepare_atomic_resolver_mid_fail_packets() {
  local iter_dir="$1"
  local blockhash="$2"
  local packet_one_path="$3"
  local packet_two_path="$4"
  local packet_batch_path="$5"
  local bridge_one_out="$6"
  local bridge_two_out="$7"

  local packet_three_path="${iter_dir}/packet_three.txt"
  local bridge_three_out="${iter_dir}/bridge_three.out"
  local tx0="${iter_dir}/atomic-resolver-tx0.txnctx"
  local tx1="${iter_dir}/atomic-resolver-tx1-bogus-lookup.txnctx"
  local tx2="${iter_dir}/atomic-resolver-tx2.txnctx"
  local gen0="${iter_dir}/atomic-resolver-tx0-gen.out"
  local gen1="${iter_dir}/atomic-resolver-tx1-gen.out"
  local gen2="${iter_dir}/atomic-resolver-tx2-gen.out"
  local first_lamports=1000000
  local second_lamports=1000001
  local third_lamports=1000002

  generate_raw_transfer_packet "${tx0}" "${packet_one_path}"   "${gen0}" "${bridge_one_out}"   "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}" "${first_lamports}" 0
  generate_raw_transfer_packet "${tx1}" "${packet_two_path}"   "${gen1}" "${bridge_two_out}"   "${blockhash}" "${TO_GENESIS_ACCOUNT_TWO}" "${second_lamports}" 1
  generate_raw_transfer_packet "${tx2}" "${packet_three_path}" "${gen2}" "${bridge_three_out}" "${blockhash}" "${TO_GENESIS_ACCOUNT_ONE}" "${third_lamports}" 0

  cat "${packet_one_path}" "${packet_two_path}" "${packet_three_path}" >"${packet_batch_path}"

  local gen_line_one gen_line_two
  gen_line_one=$(grep '^wrote synthetic txnctx ' "${gen0}" || true)
  gen_line_two=$(grep '^wrote synthetic txnctx ' "${gen1}" || true)
  [[ -n "${gen_line_one}" && -n "${gen_line_two}" ]] \
    || die "failed to capture atomic-resolver generator summaries"

  ITER_FROM=$(extract_gen_field from "${gen_line_one}")
  ITER_TO_ONE=$(extract_gen_field target "${gen_line_one}")
  ITER_TO_TWO=$(extract_gen_field target "${gen_line_two}")
  ITER_SYSTEM_KIND=$(extract_gen_field system_kind "${gen_line_one}")
  [[ -n "${ITER_FROM}" && -n "${ITER_TO_ONE}" && -n "${ITER_TO_TWO}" && "${ITER_SYSTEM_KIND}" == "transfer" ]] \
    || die "failed to parse atomic-resolver generated transaction metadata"

  ITER_PRE_FROM=$(rpc_balance "${ITER_FROM}")
  ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
  ITER_INITIAL_TO_TWO=$(rpc_balance "${ITER_TO_TWO}")
  ITER_EXPECT_TO_ONE="${ITER_INITIAL_TO_ONE}"
  ITER_EXPECT_TO_TWO="${ITER_INITIAL_TO_TWO}"
  PREPARED_INPUT_PATH="${tx0},${tx1},${tx2}"
  PREPARED_INPUT_LABEL="synthetic_atomic_resolver_mid_fail_v0_lookup_triplet"
  PREPARED_INPUT_NOTE="tx0_lamports=${first_lamports} tx1_lamports=${second_lamports} tx2_lamports=${third_lamports} pre_from=${ITER_PRE_FROM} failing_index=1 bogus_address_table_lookup=true"
}

prepare_kunorpus_raw_txn_input() {
  local iter_dir="$1"
  local iter_seed="$2"
  local packet_one_path="$3"
  local bridge_one_out="$4"
  local probe_err="$5"

  mkdir -p "${iter_dir}/kunorpus_raw_txn"

  local selected_input=""
  local selected_seed=""
  local seed_probe f
  for seed_probe in $(seq "${iter_seed}" $(( iter_seed + KUNORPUS_SEED_WINDOW - 1 ))); do
    local seed_dir="${iter_dir}/kunorpus_raw_txn/seed_${seed_probe}"
    mkdir -p "${seed_dir}"

    "${KUNORPUS}" generate txn \
      --output-dir "${seed_dir}" \
      --count "${KUNORPUS_COUNT}" \
      --seed "${seed_probe}" \
      --mutation-chance 0.0 \
      >"${iter_dir}/kunorpus_raw_txn_seed_${seed_probe}.out" 2>&1

    for f in $(find "${seed_dir}" \( -name '*.txnctx' -o -name '*.fix' \) | sort); do
      if bridge_raw_packet "${f}" "${packet_one_path}" "${bridge_one_out}" 2>"${probe_err}"; then
        selected_input="${f}"
        selected_seed="${seed_probe}"
        break 2
      fi
    done
  done

  [[ -n "${selected_input}" ]] || die "no raw kunorpus transaction input serialized successfully"

  PREPARED_INPUT_PATH="${selected_input}"
  PREPARED_INPUT_LABEL="kunorpus_raw_txn"
  PREPARED_INPUT_NOTE="requested_seed=${iter_seed} selected_seed=${selected_seed} count=${KUNORPUS_COUNT}"
  ITER_SYSTEM_KIND="raw_kunorpus_txn"
  ITER_FROM="n/a"
  ITER_TO_ONE="n/a"
  ITER_TO_TWO="n/a"
  ITER_OWNER_ONE="n/a"
  ITER_OWNER_TWO="n/a"
  ITER_SPACE_ONE="n/a"
  ITER_SPACE_TWO="n/a"
  ITER_OWNER_ONE_OBSERVED="n/a"
  ITER_OWNER_TWO_OBSERVED="n/a"
  ITER_SPACE_ONE_OBSERVED="n/a"
  ITER_SPACE_TWO_OBSERVED="n/a"
  ITER_INITIAL_TO_ONE="n/a"
  ITER_INITIAL_TO_TWO="n/a"
  ITER_EXPECT_TO_ONE="n/a"
  ITER_EXPECT_TO_TWO="n/a"
}

prepare_synthetic_input() {
  local input_path="$1"
  local gen_out="$2"
  local lamports="$3"
  local cu_limit="$4"
  local iter_seed="$5"

  local system_kind="transfer"
  local effective_lamports="${lamports}"
  local transfer_count=$((1 + (iter_seed % 4)))
  local lamports_step=$((111 + ((iter_seed * 37) % 997)))
  local cu_price="none"
  local heap_frame="none"
  local loaded_accounts_limit="none"
  local create_space=0
  local allocate_space=$((1024 + ((iter_seed % 8) * 256)))
  local owner_seed_index=$((1000 + iter_seed))
  local -a extra_args=()
  local system_selector=$((iter_seed % 10))

  if (( system_selector == 0 )); then
    system_kind="create-account"
    local min_rent_lamports
    min_rent_lamports=$(rpc_min_balance_for_rent_exemption "${create_space}")
    effective_lamports=$((min_rent_lamports + 100000 + (iter_seed % 1000)))
    extra_args+=(
      --system-kind "${system_kind}"
      --space "${create_space}"
      --owner-seed-index "${owner_seed_index}"
    )
  elif (( system_selector == 1 )); then
    system_kind="assign"
    extra_args+=(
      --system-kind "${system_kind}"
      --owner-seed-index "${owner_seed_index}"
    )
  elif (( system_selector == 2 )); then
    system_kind="allocate"
    extra_args+=(
      --system-kind "${system_kind}"
      --space "${allocate_space}"
    )
  elif (( system_selector == 3 )); then
    system_kind="create-account-with-seed"
    local min_rent_lamports
    min_rent_lamports=$(rpc_min_balance_for_rent_exemption "${create_space}")
    effective_lamports=$((min_rent_lamports + 100000 + (iter_seed % 1000)))
    extra_args+=(
      --system-kind "${system_kind}"
      --space "${create_space}"
      --owner-seed-index "${owner_seed_index}"
    )
  elif (( system_selector == 4 )); then
    system_kind="transfer-with-seed"
    extra_args+=(
      --system-kind "${system_kind}"
      --owner-seed-index "${owner_seed_index}"
    )
  else
    extra_args+=(
      --system-kind "${system_kind}"
      --transfer-count "${transfer_count}"
      --lamports-step "${lamports_step}"
    )
  fi

  if (( iter_seed % 2 == 0 )); then
    cu_price=$((1000 + (iter_seed * 97) % 50000))
    extra_args+=(--cu-price "${cu_price}")
  fi
  if (( iter_seed % 3 == 0 )); then
    heap_frame=$((32768 + ((iter_seed % 8) * 1024)))
    extra_args+=(--heap-frame "${heap_frame}")
  fi
  if (( iter_seed % 5 == 0 )); then
    loaded_accounts_limit=$((65536 + ((iter_seed % 16) * 1024)))
    extra_args+=(--loaded-accounts-data-size-limit "${loaded_accounts_limit}")
  fi

  "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
    --output "${input_path}" \
    --lamports "${effective_lamports}" \
    --cu-limit "${cu_limit}" \
    "${extra_args[@]}" \
    >"${gen_out}"
  PREPARED_INPUT_PATH="${input_path}"
  PREPARED_INPUT_LABEL="synthetic_varied_system_txnctx"
  PREPARED_INPUT_NOTE="system_kind=${system_kind} lamports=${effective_lamports} transfer_count=${transfer_count} lamports_step=${lamports_step} create_space=${create_space} allocate_space=${allocate_space} owner_seed_index=${owner_seed_index} cu_limit=${cu_limit} cu_price=${cu_price} heap_frame=${heap_frame} loaded_accounts_limit=${loaded_accounts_limit}"
}

prepare_kunorpus_system_input() {
  local iter_dir="$1"
  local iter_seed="$2"
  local probe_out="$3"
  local probe_err="$4"
  mkdir -p "${iter_dir}/kunorpus"

  local selected_input=""
  local selected_kind=""
  local selected_lamports=""
  local selected_space=""
  local selected_seed=""
  local dummy_blockhash=11111111111111111111111111111111
  local commit_multiplier=1
  case "${ITER_MODE}" in
        unique_after_reconnect|seq_id_wrap_sequence|seq_id_out_of_order_multi_batch|seq_id_wrap_out_of_order_multi_batch|disable_enable_unique_after_reconnect|disable_enable_tpu_release|url_churn_unique_after_reconnect|url_sni_churn|mixed_multi_batch|mixed_empty_multi_batch|mixed_malformed_multi_batch|mixed_bad_signature_multi_batch|mixed_bad_signature_reconnect|mixed_stale_multi_batch|mixed_stale_reconnect|mixed_terminal_producers_reconnect|random_mixed_multi_batch)
      commit_multiplier=2
      ;;
  esac
  local effective_lamports_limit=$(( KUNORPUS_MAX_TRANSFER_LAMPORTS / commit_multiplier ))
  local -a candidate_inputs=()
  local -a candidate_kinds=()
  local -a candidate_lamports=()
  local -a candidate_spaces=()
  local -a candidate_seeds=()
  local seed_probe
  for seed_probe in $(seq "${iter_seed}" $(( iter_seed + KUNORPUS_SEED_WINDOW - 1 ))); do
    local seed_dir="${iter_dir}/kunorpus/seed_${seed_probe}"
    mkdir -p "${seed_dir}"

    "${KUNORPUS}" generate instr system \
      --output-dir "${seed_dir}" \
      --count "${KUNORPUS_COUNT}" \
      --seed "${seed_probe}" \
      --mutation-chance 0.0 \
      >"${iter_dir}/kunorpus_seed_${seed_probe}.out" 2>&1

    for f in $(find "${seed_dir}" -name '*.instrctx' | sort); do
      if "${BRIDGE_BIN}" \
          --input "${f}" \
          --output "${iter_dir}/probe-packets.txt" \
          --adapt-mode local-system-transfer \
          --recent-blockhash "${dummy_blockhash}" \
          >"${probe_out}" 2>"${probe_err}"; then
        local adapt_line
        adapt_line=$(grep '^adapted family=' "${probe_out}" || true)
        local system_kind
        system_kind=$(extract_bridge_field system_kind "${adapt_line}")
        local lamports
        lamports=$(sed -n 's/.* lamports=\([0-9][0-9]*\) .*/\1/p' "${probe_out}")
        if [[ "${KUNORPUS_SYSTEM_KIND}" != "any" && "${system_kind}" != "${KUNORPUS_SYSTEM_KIND}" ]]; then
          continue
        fi

        if [[ -n "${lamports}" && "${lamports}" -le "${effective_lamports_limit}" ]]; then
          local candidate_space="0"
          if [[ "${system_kind}" == "create_account" || "${system_kind}" == "create_account_with_seed" || "${system_kind}" == "allocate" ]]; then
            local space
            space=$(extract_bridge_field space "${adapt_line}")
            candidate_space="${space}"
            if [[ "${system_kind}" == "create_account" || "${system_kind}" == "create_account_with_seed" ]]; then
              local min_rent_lamports
              min_rent_lamports=$(rpc_min_balance_for_rent_exemption "${space}")
              if (( lamports < min_rent_lamports )); then
                continue
              fi
            fi
          fi

          candidate_inputs+=("${f}")
          candidate_kinds+=("${system_kind}")
          candidate_lamports+=("${lamports}")
          candidate_spaces+=("${candidate_space}")
          candidate_seeds+=("${seed_probe}")
        fi
      fi
    done
  done

  local candidate_count=${#candidate_inputs[@]}
  (( candidate_count > 0 )) || die "no executable-compatible kunorpus system transaction found for seed window ${iter_seed}..$(( iter_seed + KUNORPUS_SEED_WINDOW - 1 )) count ${KUNORPUS_COUNT}"

  local selected_index=0
  local selected_bucket_size=${candidate_count}
  local present_kind_count=1
  local present_kind_csv="${KUNORPUS_SYSTEM_KIND}"
  if [[ "${KUNORPUS_SYSTEM_KIND}" == "any" ]]; then
    local -a supported_kinds=(
      transfer
      assign
      allocate
      create_account
      transfer_with_seed
      create_account_with_seed
    )
    local -a present_kinds=()
    local kind candidate_kind idx
    for kind in "${supported_kinds[@]}"; do
      for candidate_kind in "${candidate_kinds[@]}"; do
        if [[ "${candidate_kind}" == "${kind}" ]]; then
          present_kinds+=("${kind}")
          break
        fi
      done
    done

    present_kind_count=${#present_kinds[@]}
    (( present_kind_count > 0 )) || die "compatible kunorpus system candidate pool had no supported family buckets"
    selected_kind="${present_kinds[$(( iter_seed % present_kind_count ))]}"

    local -a matching_indices=()
    for idx in "${!candidate_kinds[@]}"; do
      if [[ "${candidate_kinds[$idx]}" == "${selected_kind}" ]]; then
        matching_indices+=("${idx}")
      fi
    done

    selected_bucket_size=${#matching_indices[@]}
    (( selected_bucket_size > 0 )) || die "selected compatible family bucket ${selected_kind} was empty"
    selected_index="${matching_indices[$(( (iter_seed / present_kind_count) % selected_bucket_size ))]}"
    present_kind_csv=$(IFS=,; printf '%s' "${present_kinds[*]}")
  else
    selected_index=$(( iter_seed % candidate_count ))
    selected_kind="${candidate_kinds[$selected_index]}"
  fi

  selected_input="${candidate_inputs[$selected_index]}"
  selected_kind="${candidate_kinds[$selected_index]}"
  selected_lamports="${candidate_lamports[$selected_index]}"
  selected_space="${candidate_spaces[$selected_index]}"
  selected_seed="${candidate_seeds[$selected_index]}"

  "${BRIDGE_BIN}"     --input "${selected_input}"     --output "${iter_dir}/probe-packets.txt"     --adapt-mode local-system-transfer     --recent-blockhash "${dummy_blockhash}"     >"${probe_out}" 2>"${probe_err}"
  cp "${probe_out}" "${iter_dir}/selected-probe.out"

  PREPARED_INPUT_PATH="${selected_input}"
  PREPARED_INPUT_LABEL="kunorpus_system_instrctx"
  PREPARED_INPUT_NOTE="requested_seed=${iter_seed} requested_kind=${KUNORPUS_SYSTEM_KIND} required_commit_count=${commit_multiplier} effective_lamports_limit=${effective_lamports_limit} candidate_count=${candidate_count} present_kind_count=${present_kind_count} present_kinds=${present_kind_csv} selected_bucket_size=${selected_bucket_size} selected_index=${selected_index} selected_seed=${selected_seed} selected_kind=${selected_kind} count=${KUNORPUS_COUNT} selected_lamports=${selected_lamports} selected_space=${selected_space}"
}

prepare_kunorpus_vote_input() {
  local iter_dir="$1"
  local iter_seed="$2"
  local blockhash="$3"
  local packet_one_path="$4"
  local bridge_one_out="$5"
  local probe_err="$6"

  [[ -f "${NODE_KEYPAIR_PATH}" ]] || die "missing node keypair ${NODE_KEYPAIR_PATH}"
  [[ -f "${VOTE_KEYPAIR_PATH}" ]] || die "missing vote keypair ${VOTE_KEYPAIR_PATH}"
  [[ -f "${AUTHORIZED_VOTER_KEYPAIR_PATH}" ]] || die "missing authorized voter keypair ${AUTHORIZED_VOTER_KEYPAIR_PATH}"

  mkdir -p "${iter_dir}/kunorpus_vote"

  local slot_hashes_b64="${iter_dir}/slot_hashes.b64"
  local slot_hashes_out="${iter_dir}/slot_hashes.out"
  local slot_hashes_ready=0
  for _ in $(seq 1 120); do
    if rpc_account_base64 "${SLOT_HASHES_SYSVAR}" >"${slot_hashes_b64}" 2>/dev/null; then
      slot_hashes_ready=1
      break
    fi
    sleep 1
  done
  [[ "${slot_hashes_ready}" == "1" ]] || die "slot-hashes sysvar account did not become available"

  "${DECODE_SLOT_HASHES_BIN}" --input "${slot_hashes_b64}" --base64 >"${slot_hashes_out}"

  local vote_slot vote_hash
  vote_slot=$(sed -n 's/^slot=\([0-9][0-9]*\) hash=.*/\1/p' "${slot_hashes_out}" | head -n 1)
  vote_hash=$(sed -n 's/^slot=[0-9][0-9]* hash=\([^ ]*\) .*/\1/p' "${slot_hashes_out}" | head -n 1)
  [[ -n "${vote_slot}" && -n "${vote_hash}" ]] || die "failed to parse latest slot-hash entry"

  local selected_input=""
  local selected_seed=""
  local seed_probe f
  for seed_probe in $(seq "${iter_seed}" $(( iter_seed + KUNORPUS_SEED_WINDOW - 1 ))); do
    local seed_dir="${iter_dir}/kunorpus_vote/seed_${seed_probe}"
    mkdir -p "${seed_dir}"

    "${KUNORPUS}" generate instr vote \
      --output-dir "${seed_dir}" \
      --count "${KUNORPUS_COUNT}" \
      --seed "${seed_probe}" \
      --mutation-chance 0.0 \
      >"${iter_dir}/kunorpus_vote_seed_${seed_probe}.out" 2>&1

    for f in $(find "${seed_dir}" -name '*.instrctx' | sort); do
      if "${BRIDGE_BIN}" \
          --input "${f}" \
          --output "${packet_one_path}" \
          --adapt-mode local-simple-vote \
          --recent-blockhash "${blockhash}" \
          --node-keypair "${NODE_KEYPAIR_PATH}" \
          --vote-keypair "${VOTE_KEYPAIR_PATH}" \
          --authorized-voter-keypair "${AUTHORIZED_VOTER_KEYPAIR_PATH}" \
          --vote-slot "${vote_slot}" \
          --vote-hash "${vote_hash}" \
          >"${bridge_one_out}" 2>"${probe_err}"; then
        selected_input="${f}"
        selected_seed="${seed_probe}"
        break 2
      fi
    done
  done

  [[ -n "${selected_input}" ]] || die "no vote-family input adapted successfully"

  local adapt_line
  adapt_line=$(grep '^adapted family=local-simple-vote ' "${bridge_one_out}" || true)
  [[ -n "${adapt_line}" ]] || die "bridge did not emit local-simple-vote summary"

  ITER_SIMPLE_VOTE_NODE=$(extract_bridge_field node "${adapt_line}")
  ITER_VOTE_ACCOUNT=$(extract_bridge_field vote "${adapt_line}")
  ITER_AUTHORIZED_VOTER=$(extract_bridge_field authorized_voter "${adapt_line}")
  ITER_VOTE_SLOT=$(extract_bridge_field vote_slot "${adapt_line}")
  ITER_VOTE_HASH=$(extract_bridge_field vote_hash "${adapt_line}")
  ITER_SYSTEM_KIND="simple_vote"
  ITER_FROM="${ITER_SIMPLE_VOTE_NODE}"
  ITER_TO_ONE="${ITER_VOTE_ACCOUNT}"
  ITER_TO_TWO="${ITER_VOTE_ACCOUNT}"
  ITER_INITIAL_TO_ONE="n/a"
  ITER_INITIAL_TO_TWO="n/a"
  ITER_EXPECT_TO_ONE="n/a"
  ITER_EXPECT_TO_TWO="n/a"

  rpc_vote_account_json "${ITER_VOTE_ACCOUNT}" >"${iter_dir}/pre_vote.json"
  if jq -e '.error.message // "" | contains("jsonParsed is unsupported")' "${iter_dir}/pre_vote.json" >/dev/null 2>&1; then
    ITER_VOTE_ACCOUNT_JSON_UNSUPPORTED=1
    ITER_PRE_AUTHORIZED_VOTER="jsonParsed_unsupported"
    ITER_PRE_VOTES_LEN="jsonParsed_unsupported"
    ITER_PRE_LAST_TIMESTAMP_SLOT="jsonParsed_unsupported"
    echo "  note: validator RPC does not support jsonParsed vote-account state; skipping vote-account history invariant"
  else
    ITER_PRE_AUTHORIZED_VOTER=$(jq -r '.result.value.data.parsed.info.authorizedVoters[0].authorizedVoter' "${iter_dir}/pre_vote.json")
    ITER_PRE_VOTES_LEN=$(jq -r '.result.value.data.parsed.info.votes | length' "${iter_dir}/pre_vote.json")
    ITER_PRE_LAST_TIMESTAMP_SLOT=$(jq -r '.result.value.data.parsed.info.lastTimestamp.slot' "${iter_dir}/pre_vote.json")
    [[ "${ITER_PRE_AUTHORIZED_VOTER}" == "${ITER_AUTHORIZED_VOTER}" ]] || die "vote account authorized voter ${ITER_PRE_AUTHORIZED_VOTER} did not match adapted signer ${ITER_AUTHORIZED_VOTER}"
  fi

  PREPARED_INPUT_PATH="${selected_input}"
  PREPARED_INPUT_LABEL="kunorpus_vote_instrctx"
  PREPARED_INPUT_NOTE="requested_seed=${iter_seed} selected_seed=${selected_seed} count=${KUNORPUS_COUNT} vote_slot=${vote_slot} vote_hash=${vote_hash} node=${ITER_SIMPLE_VOTE_NODE} vote=${ITER_VOTE_ACCOUNT} authorized_voter=${ITER_AUTHORIZED_VOTER}"
}

prepare_input_source() {
  local family="$1"
  local iter_dir="$2"
  local iter_seed="$3"
  local input_path="$4"
  local gen_out="$5"
  local lamports="$6"
  local cu_limit="$7"
  local probe_out="$8"
  local probe_err="$9"
  case "${family}" in
    synthetic)
      prepare_synthetic_input "${input_path}" "${gen_out}" "${lamports}" "${cu_limit}" "${iter_seed}"
      ;;
    kunorpus_system)
      prepare_kunorpus_system_input "${iter_dir}" "${iter_seed}" "${probe_out}" "${probe_err}"
      ;;
    *)
      die "unsupported input family ${family}"
      ;;
  esac
}

random_mixed_pattern_for_seed() {
  local iter_seed="$1"
  case $((iter_seed % 4)) in
    0)
      printf '%s\n' 'valid1,replay1,empty,valid2,stale2'
      ;;
    1)
      printf '%s\n' 'empty,valid1,stale2,valid2,replay2'
      ;;
    2)
      printf '%s\n' 'valid1,valid2,replay1,replay2'
      ;;
    *)
      printf '%s\n' 'stale1,empty,valid1,valid2,replay1'
      ;;
  esac
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

random_mixed_valid2_offset() {
  local pattern="$1"
  local token
  local idx=0
  local -a tokens=()
  local IFS=,
  read -r -a tokens <<< "${pattern}"
  for token in "${tokens[@]}"; do
    if [[ "${token}" == "valid2" ]]; then
      printf '%s\n' "${idx}"
      return 0
    fi
    idx=$((idx + 1))
  done
  die "random mixed pattern is missing valid2: ${pattern}"
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

render_scenario() {
  local mode="$1"
  local scenario_path="$2"
  local packet_one="$3"
  local packet_two="$4"
  local packet_batch="$5"
  local seq_one="$6"
  local seq_two="$7"
  local iter_seed="$8"
  case "${mode}" in
    commit_once|seq_id_max_once|bam_fee_priority_commit|fee_only_commit|durable_nonce_commit)
	      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit one executable BAM transaction."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    durable_nonce_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one durable-nonce BAM transaction, close immediately, reconnect, and require the committed result to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit one durable-nonce BAM transaction, close conn=1, resume on conn=2, and replay the same signed packet with a fresh BAM seq_id."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    durable_nonce_wrong_authority)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one durable-nonce BAM transaction signed by the wrong nonce authority and require one not-committed result."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    durable_nonce_wrong_authority_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one wrong-authority durable-nonce BAM transaction, close immediately, reconnect, and require the failure result to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit one BAM priority-fee transaction, close conn=1, resume on conn=2, and replay the same signed packet with a fresh BAM seq_id."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    fee_only_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one fee-only BAM transaction, close immediately, reconnect, and require the committed result to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
	    bam_fee_url_churn_priority_commit|bam_fee_url_churn_same_slot_priority_commit)
	      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit one priority-fee BAM transaction with the first BAM fee config, then idle while the operator switches to a second BAM URL/config."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 600000
EOF2
      ;;
    bam_fee_config_refresh_priority_commit|bam_fee_config_commission_refresh_priority_commit)
      local refreshed_fee_recipient="${ITER_TO_ONE}"
      local refresh_description="Seed ${iter_seed}: commit one priority-fee BAM transaction, mutate BamConfig on the same scheduler, wait for GetBuilderConfig refresh, then commit a second priority-fee BAM transaction."
      if [[ "${mode}" == "bam_fee_config_commission_refresh_priority_commit" ]]; then
        refreshed_fee_recipient="${ITER_TO_TWO}"
        refresh_description="Seed ${iter_seed}: commit one priority-fee BAM transaction, change only BamConfig commission on the same scheduler, wait for GetBuilderConfig refresh, then commit a second priority-fee BAM transaction."
      fi
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "${refresh_description}"
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

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
	    bam_fee_config_refresh_queue_burst|bam_fee_config_refresh_source_mix_queue_burst)
	      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit one priority-fee BAM transaction under the initial fee config, mutate BamConfig, then send a priority-fee queue burst and close the scheduler stream."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "config_update"
prio_fee_recipient_pubkey = "${ITER_TO_ONE}"
commission_bps = 700
include_block_engine_config = false

[[events]]
type = "sleep"
ms = 1500

EOF2
      local batch_idx
      for batch_idx in $(seq 0 $((ITER_QUEUE_BURST_BATCH_COUNT - 1))); do
        local seq_id=$((seq_two + batch_idx))
        local packet_file="${ITER_QUEUE_PACKET_FILES[$batch_idx]}"
        cat >> "${scenario_path}" <<EOF2
[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_file}"
revert_on_error = true
simple_vote_tx = false

EOF2
        write_max_schedule_slot_toml "${ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT}" >> "${scenario_path}"
        cat >> "${scenario_path}" <<EOF2

EOF2
        if (( ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS > 0 && batch_idx + 1 == ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS )); then
          cat >> "${scenario_path}" <<EOF2
[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = $((1 + ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS))
timeout_ms = 30000

EOF2
        fi
      done
      cat >> "${scenario_path}" <<EOF2
[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bam_fee_config_midqueue_refresh|bam_fee_config_midqueue_source_mix_queue_burst|bam_fee_config_midqueue_source_mix_multi_reconnect)
      local midqueue_prefix_count=$((ITER_QUEUE_BURST_BATCH_COUNT / 2))
      local midqueue_observe_count="${ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS}"
      (( midqueue_prefix_count < 1 )) && midqueue_prefix_count=1
      if ! [[ "${midqueue_observe_count}" =~ ^[0-9]+$ ]] || (( midqueue_observe_count < 1 )); then
        midqueue_observe_count=1
      fi
      (( midqueue_observe_count > midqueue_prefix_count )) && midqueue_observe_count="${midqueue_prefix_count}"
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a priority-fee queue prefix under the initial BAM fee config, mutate BamConfig mid-queue, then send the suffix and close the scheduler stream."
heartbeat_interval_ms = 1000
replay_on_reconnect = false
$( [[ "${mode}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]] && printf 'resume_on_reconnect = true\n' )

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

EOF2
      local batch_idx
      for batch_idx in $(seq 0 $((midqueue_prefix_count - 1))); do
        local seq_id=$((seq_two + batch_idx))
        local packet_file="${ITER_QUEUE_PACKET_FILES[$batch_idx]}"
        cat >> "${scenario_path}" <<EOF2
[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_file}"
revert_on_error = true
simple_vote_tx = false

EOF2
        write_max_schedule_slot_toml "${ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT}" >> "${scenario_path}"
        cat >> "${scenario_path}" <<EOF2

EOF2
      done
      cat >> "${scenario_path}" <<EOF2
[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = $((1 + midqueue_observe_count))
timeout_ms = 30000

[[events]]
type = "config_update"
prio_fee_recipient_pubkey = "${ITER_TO_ONE}"
commission_bps = 700
include_block_engine_config = false

[[events]]
type = "sleep"
ms = 1500

EOF2
      for batch_idx in $(seq "${midqueue_prefix_count}" $((ITER_QUEUE_BURST_BATCH_COUNT - 1))); do
        local seq_id=$((seq_two + batch_idx))
        local packet_file="${ITER_QUEUE_PACKET_FILES[$batch_idx]}"
        cat >> "${scenario_path}" <<EOF2
[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_file}"
revert_on_error = true
simple_vote_tx = false

EOF2
        write_max_schedule_slot_toml "${ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT}" >> "${scenario_path}"
        cat >> "${scenario_path}" <<EOF2

EOF2
      done
      if [[ "${mode}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
        cat >> "${scenario_path}" <<EOF2
[[events]]
type = "close_stream"

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = ${ITER_QUEUE_BURST_SECOND_CLOSE_AFTER_RESULTS}
timeout_ms = 30000

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 2000
EOF2
      else
        cat >> "${scenario_path}" <<EOF2
[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 1000
EOF2
      fi
      ;;
    disable_enable_queue_burst_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a BAM queue burst, disable BAM after a result prefix, re-enable BAM, and require queued terminal results to survive the operator-induced reconnect."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

EOF2
      local batch_idx
      for batch_idx in $(seq 0 $((ITER_QUEUE_BURST_BATCH_COUNT - 1))); do
        local seq_id=$((seq_one + batch_idx))
        local packet_file="${ITER_QUEUE_PACKET_FILES[$batch_idx]}"
        cat >> "${scenario_path}" <<EOF2
[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_file}"
revert_on_error = true
simple_vote_tx = false

EOF2
        write_max_schedule_slot_toml "${ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT}" >> "${scenario_path}"
        cat >> "${scenario_path}" <<EOF2

EOF2
        if (( ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS > 0 && batch_idx + 1 == ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS )); then
          cat >> "${scenario_path}" <<EOF2
[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = ${ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS}
timeout_ms = 30000

EOF2
        fi
      done
      cat >> "${scenario_path}" <<EOF2
[[events]]
type = "sleep"
ms = 600000
EOF2
      ;;
    quarantine_disable_enable_queue_inflight|quarantine_url_churn_queue_inflight)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a BAM queue burst, change scheduler generation while results are in flight, and require old-generation result quarantine."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

EOF2
      local batch_idx
      for batch_idx in $(seq 0 $((ITER_QUEUE_BURST_BATCH_COUNT - 1))); do
        local seq_id=$((seq_one + batch_idx))
        local packet_file="${ITER_QUEUE_PACKET_FILES[$batch_idx]}"
        cat >> "${scenario_path}" <<EOF2
[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_file}"
revert_on_error = true
simple_vote_tx = false

EOF2
        write_max_schedule_slot_toml "${ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT}" >> "${scenario_path}"
        cat >> "${scenario_path}" <<EOF2

[[events]]
type = "sleep"
ms = 2

EOF2
      done
      cat >> "${scenario_path}" <<EOF2
[[events]]
type = "sleep"
ms = 600000
EOF2
      ;;
    valid_alt_commit)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit a v0 transfer that references a preseeded address lookup table."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    invalid_alt_missing_table)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: reject a signed v0 transfer whose address lookup table account is missing."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    invalid_alt_missing_table_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a signed v0 transfer with a missing address lookup table, close immediately, reconnect, and require one durable rejection."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one valid transfer with an intentionally tiny compute-unit limit and require a transaction-error BAM result."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bam_cu_limit_fail_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one valid transfer with an intentionally tiny compute-unit limit, close the scheduler stream immediately, and require the transaction-error BAM result to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    source_mix_bam_tpu|source_mix_duplicate_tpu_after_bam)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit one executable BAM transaction, then keep BAM override active while the harness injects a normal TPU transaction."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 5000
EOF2
      ;;
    source_mix_precommit)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: keep BAM override active, inject a normal TPU transaction before any BAM commit, then commit one BAM transaction."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 5000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    disable_enable_tpu_release)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit one BAM transaction, disable BAM so a normal TPU packet can land, then re-enable BAM and require the scheduler stream to reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 9000
EOF2
      ;;
    partial_drain_reconnect|queue_burst_reconnect|queue_burst64_reconnect|queue_burst64_leader_plus1_reconnect|schedule_boundary_jitter|queue_reconnect_timing_jitter|queue_burst_multi_reconnect|source_mix_queue_burst_multi_reconnect|queue_burst128_reconnect|queue_burst256_reconnect|queue_burst512_reconnect|queue_burst_leader_reconnect|queue_burst64_leader_reconnect|bam_fee_queue_burst_reconnect|bam_fee_source_mix_queue_burst_reconnect|source_mix_queue_burst_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a queue burst, wait for a configured prefix, close the scheduler stream, and rely on durable BAM results across reconnect."
heartbeat_interval_ms = 1000
replay_on_reconnect = false
EOF2
      if [[ "${mode}" == "queue_burst_multi_reconnect" || "${mode}" == "source_mix_queue_burst_multi_reconnect" ]]; then
        cat >> "${scenario_path}" <<EOF2
resume_on_reconnect = true
EOF2
      fi
      cat >> "${scenario_path}" <<EOF2

[[events]]
type = "sleep"
ms = 1000

EOF2
      local batch_idx
      for batch_idx in $(seq 0 $((ITER_QUEUE_BURST_BATCH_COUNT - 1))); do
        local seq_id=$((seq_one + batch_idx))
        local packet_file="${ITER_QUEUE_PACKET_FILES[$batch_idx]}"
        cat >> "${scenario_path}" <<EOF2
[[events]]
type = "send_batch"
seq_id = ${seq_id}
packets_base64_file = "${packet_file}"
revert_on_error = true
simple_vote_tx = false

EOF2
        write_max_schedule_slot_toml "${ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT}" >> "${scenario_path}"
        cat >> "${scenario_path}" <<EOF2

EOF2
        if (( ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS > 0 && batch_idx + 1 == ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS )); then
          cat >> "${scenario_path}" <<EOF2
[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = ${ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS}
timeout_ms = 30000

EOF2
        fi
      done
      if [[ "${mode}" == "queue_burst_multi_reconnect" || "${mode}" == "source_mix_queue_burst_multi_reconnect" ]]; then
        cat >> "${scenario_path}" <<EOF2
[[events]]
type = "close_stream"

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = ${ITER_QUEUE_BURST_SECOND_CLOSE_AFTER_RESULTS}
timeout_ms = 30000

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 2000
EOF2
      else
        cat >> "${scenario_path}" <<EOF2
[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 1000
EOF2
      fi
      ;;
    replay_same_conn)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit one executable BAM System-program transaction, then replay it on the same scheduler stream."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    replay_after_reconnect|seq_id_max_replay_after_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit one executable BAM System-program transaction, close conn=1, resume on conn=2, and replay it."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    unique_after_reconnect|seq_id_wrap_sequence)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit one executable BAM System-program transaction, close conn=1, resume on conn=2, and commit a second unique one."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    vote_reject_once)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one locally signed simple vote and confirm BAM rejects it before validator decode."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = true

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    vote_reject_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one locally signed simple vote, close immediately, reconnect, and require exactly one VOTE_TRANSACTION_FAILURE rejection."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = true

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
    raw_kunorpus_once)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one raw kunorpus transaction-context packet and record the terminal BAM result."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    raw_kunorpus_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one raw kunorpus transaction-context packet, close immediately, reconnect, and require exactly one terminal BAM result."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
    stale_slot_reject)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one locally valid BAM System-program transaction whose max_schedule_slot is already stale."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = 0
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    stale_slot_reject_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one locally valid BAM System-program transaction whose max_schedule_slot is already stale, close immediately, reconnect, and require exactly one scheduling rejection."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a zero-packet BAM batch and confirm it is rejected as EMPTY."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packet_count = 0
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    empty_batch_reject_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a zero-packet BAM batch, close immediately, reconnect, and require exactly one EMPTY rejection."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
    malformed_first_atomic)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a malformed packet followed by a valid packet in one revert_on_error=true BAM batch."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    malformed_first_atomic_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a malformed-first atomic BAM batch, close the scheduler stream immediately, and require the single terminal rejection to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    malformed_tail_atomic)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a valid packet followed by a malformed packet in one revert_on_error=true BAM batch."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    malformed_tail_atomic_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a valid packet followed by a malformed packet in one revert_on_error=true BAM batch, close immediately, and require the single terminal rejection to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bad_signature_first_atomic)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a parse-valid bad-signature packet followed by a valid packet in one revert_on_error=true BAM batch."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bad_signature_first_atomic_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a parse-valid bad-signature packet followed by a valid packet in one revert_on_error=true BAM batch, close immediately, and require the single terminal rejection to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bad_signature_tail_atomic)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a valid packet followed by a parse-valid bad-signature packet in one revert_on_error=true BAM batch."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    bad_signature_tail_atomic_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a valid packet followed by a parse-valid bad-signature packet in one revert_on_error=true BAM batch, close immediately, and require the single terminal rejection to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    non_atomic_single_packet)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one valid System-program packet in a revert_on_error=false BAM batch."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = false
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    mixed_multi_batch)
      local seq_mid=$((seq_one + 1))
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one scheduler message containing a valid BAM packet, the same packet replayed, and a second valid packet."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    duplicate_seq_split)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send six valid transfers as a [5,1] same-seq_id split."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_split_batch"
seq_id = ${seq_one}
splits = [5, 1]
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 2
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    duplicate_seq_split_reconnect)
      local packet_first5="${packet_batch%.txt}.first5.txt"
      local packet_last="${packet_batch%.txt}.last1.txt"
      sed -n '1,5p' "${packet_batch}" >"${packet_first5}"
      sed -n '6p' "${packet_batch}" >"${packet_last}"
      [[ -s "${packet_first5}" && -s "${packet_last}" ]] \
        || die "failed to split duplicate_seq_split_reconnect packet batch"
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a five-transfer BAM batch, close conn=1, then send a one-transfer batch with the same seq_id on conn=2."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_first5}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 500

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_last}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    seq_id_out_of_order_multi_batch|seq_id_wrap_out_of_order_multi_batch)
      local revert_on_error="true"
      if [[ "${mode}" == "seq_id_wrap_out_of_order_multi_batch" ]]; then
        revert_on_error="false"
      fi
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one scheduler message with two valid BAM batches in descending seq_id order."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_two}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = ${revert_on_error}
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_one}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = ${revert_on_error}
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 2
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    seq_id_wrap_conflicting_spend_multi_batch)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send two conflicting single-transaction BAM batches across the u32 seq_id wrap boundary."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_two}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = false
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_one}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = false
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 2
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    mixed_empty_multi_batch)
      local seq_mid=$((seq_one + 1))
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one scheduler message containing a valid packet, an empty batch, and a second valid packet."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
	      ;;
	    mixed_malformed_multi_batch)
	      local seq_mid=$((seq_one + 1))
	      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one scheduler message containing a valid packet, a malformed batch, and a second valid packet."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_mid}
packets_base64_file = "${packet_batch}"
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
timeout_ms = 30000

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
	      ;;
	    mixed_bad_signature_multi_batch)
	      local seq_mid=$((seq_one + 1))
	      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one scheduler message containing a valid packet, a bad-signature batch, and a second valid packet."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_mid}
packets_base64_file = "${packet_batch}"
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
timeout_ms = 30000

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
	      ;;
    mixed_bad_signature_reconnect)
      local seq_mid=$((seq_one + 1))
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send valid, bad-signature, and second-valid batches in one scheduler message, close after the bad-signature result, and require durable commits across reconnect."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events.batches]]
seq_id = ${seq_mid}
packets_base64_file = "${packet_batch}"
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
timeout_ms = 30000

[[events]]
type = "close_stream"
EOF2
      ;;
	    mixed_stale_multi_batch)
	      local seq_mid=$((seq_one + 1))
	      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send one scheduler message containing a valid packet, a stale batch, and a second valid packet."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    mixed_stale_reconnect)
      local seq_mid=$((seq_one + 1))
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send valid, stale, and second-valid batches in one scheduler message, close after the stale result, and require durable commits across reconnect."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "close_stream"
EOF2
      ;;
    mixed_terminal_producers_reconnect)
      local seq_malformed=$((seq_one + 1))
      local seq_stale=$((seq_one + 2))
      local seq_bad_sig=$((seq_one + 3))
      local malformed_packet="${packet_batch%.txt}.malformed.txt"
      local bad_signature_packet="${packet_batch%.txt}.bad_signature.txt"
      printf '%s\n' 'AA==' >"${malformed_packet}"
      write_bad_signature_packet "${packet_one}" "${bad_signature_packet}"
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send valid, malformed, stale, bad-signature, and second-valid batches in one scheduler message, then close after the first rejection."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_multi_batch"

[[events.batches]]
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "close_stream"
EOF2
      ;;
    random_mixed_multi_batch)
      local pattern="${ITER_RANDOM_MIX_PATTERN:-}"
      [[ -n "${pattern}" ]] || pattern=$(random_mixed_pattern_for_seed "${iter_seed}")
      local committed_count
      local not_committed_count
      committed_count=$(random_mixed_token_count "${pattern}" committed)
      not_committed_count=$(random_mixed_token_count "${pattern}" not_committed)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode} pattern=${pattern}
description = "Seed ${iter_seed}: send a seed-selected scheduler message mixing valid, replayed, empty, and stale BAM batches."
heartbeat_interval_ms = 1000
replay_on_reconnect = false

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_multi_batch"

EOF2
      append_random_mixed_batches "${scenario_path}" "${pattern}" "${packet_one}" "${packet_two}" "${seq_one}"
      cat >> "${scenario_path}" <<EOF2
[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = ${committed_count}
timeout_ms = 30000

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = ${not_committed_count}
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    seq_collision_same_conn)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit one executable BAM System-program transaction, then send a different executable payload with the same seq_id on the same scheduler stream."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 2
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    seq_collision_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit one executable BAM System-program transaction, reconnect, then send a different executable payload with the same seq_id."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "close_stream"

[[events]]
type = "sleep"
ms = 500

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_two}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    non_atomic_inconsistent_bundle)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send two independent transfers where the first succeeds and leaves the second without enough funds."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = false
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    non_atomic_partial_overdraft_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a revert_on_error=false partial-overdraft BAM batch, close immediately, reconnect, and require one terminal result on either side of the reconnect race."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = false
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
timeout_ms = 5000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    non_atomic_first_overdraft|non_atomic_mid_overdraft|non_atomic_partial_overdraft|non_atomic_partial_resolver_fail|non_atomic_partial_blockhash_fail|non_atomic_partial_duplicate_sig|non_atomic_partial_cu_fail)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send a revert_on_error=false BAM batch with valid work around a targeted failing transaction."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = false
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    atomic_first_overdraft)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send two transfer packets in one revert_on_error=true BAM batch; the first packet overdraws and the valid suffix must not execute."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    atomic_first_overdraft_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send an atomic first-overdraft batch, close the scheduler stream immediately, and require the single terminal rejection to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    atomic_revert)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send two transfer packets in one revert_on_error=true BAM batch; the second packet overdraws and the full batch must revert."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    atomic_revert_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send an atomic-revert BAM batch, close the scheduler stream immediately, and require the single terminal rejection to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    atomic_mid_fail)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send three transfer packets in one revert_on_error=true BAM batch; the middle packet overdraws and the full batch must revert."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    atomic_mid_fail_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send an atomic valid-overdraft-valid batch, close the scheduler stream immediately, and require the single terminal rejection to survive reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    atomic_blockhash_mid_fail)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send three transfer packets in one revert_on_error=true BAM batch; the middle packet uses an expired blockhash and the full batch must reject at index 1."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
      ;;
    atomic_blockhash_mid_fail_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send an atomic expired-blockhash middle-failure batch, close the scheduler stream immediately, and require one terminal rejection across reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    atomic_resolver_mid_fail)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send three packets in one revert_on_error=true BAM batch; the middle v0 packet has a bogus address-table lookup and must reject the full batch at index 1."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
      ;;
    atomic_resolver_mid_fail_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send an atomic bogus-ALT middle-failure batch, close the scheduler stream immediately, and require one terminal rejection across reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    atomic_duplicate_sig_mid_fail)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send three packets in one revert_on_error=true BAM batch; the middle packet duplicates tx0's signature and the full batch must reject without landing the valid suffix."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
      ;;
    atomic_duplicate_sig_mid_fail_reconnect)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send an atomic duplicate-signature middle-failure batch, close the scheduler stream immediately, and require one terminal rejection across reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    source_mix_atomic_revert_precommit)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: keep BAM override active, inject a normal TPU transaction before any BAM result, then send an atomic BAM batch that must revert at index 1."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 5000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "not_committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    non_atomic_valid_multi_packet)
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: send two valid System-program packets in one revert_on_error=false BAM batch."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_batch}"
max_schedule_slot = "max"
revert_on_error = false
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "batch_result"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    disable_enable_unique_after_reconnect|url_churn_unique_after_reconnect|url_sni_churn)
      local reconnect_sleep_ms=3000
      if [[ "${mode}" == "url_sni_churn" ]]; then
        reconnect_sleep_ms=7000
      fi
      cat > "${scenario_path}" <<EOF2
# seed=${iter_seed} mode=${mode}
description = "Seed ${iter_seed}: commit one executable BAM System-program transaction, apply BAM control-plane churn, then commit a second unique one after reconnect."
heartbeat_interval_ms = 1000
resume_on_reconnect = true

[[events]]
type = "sleep"
ms = 1000

[[events]]
type = "send_batch"
seq_id = ${seq_one}
packets_base64_file = "${packet_one}"
max_schedule_slot = "max"
revert_on_error = true
simple_vote_tx = false

[[events]]
type = "wait_inbound"
kind = "committed_batch"
min_count = 1
timeout_ms = 30000

[[events]]
type = "sleep"
ms = ${reconnect_sleep_ms}

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
timeout_ms = 30000

[[events]]
type = "sleep"
ms = 1000
EOF2
      ;;
    *)
      die "unsupported scenario mode ${mode}"
      ;;
  esac
}

render_operator_events() {
  local mode="$1"
  local events_path="$2"
  local seq_one="$3"
  case "${mode}" in
    disable_enable_unique_after_reconnect)
      cat > "${events_path}" <<EOF2
wait_log bam 30000 scheduler<-validator batch_result seq_id=${seq_one} status=committed
set_bam --disable
wait_log bam 45000 scheduler stream closed by validator conn=1
set_bam --enable
wait_log_count bam 45000 2 InitSchedulerStream:
wait_log bam 45000 scheduler<-validator auth proof
EOF2
      ;;
    disable_enable_queue_burst_reconnect)
      local disable_prefix="${ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS:-1}"
      if ! [[ "${disable_prefix}" =~ ^[0-9]+$ ]] || (( disable_prefix < 1 )); then
        disable_prefix=1
      fi
      cat > "${events_path}" <<EOF2
wait_log_count bam 45000 ${disable_prefix} scheduler<-validator batch_result seq_id=
set_bam --disable
wait_log bam 45000 scheduler stream closed by validator conn=1
set_bam --enable
wait_log_count bam 45000 2 InitSchedulerStream:
wait_log bam 45000 scheduler<-validator auth proof
EOF2
      ;;
    quarantine_disable_enable_queue_inflight)
      cat > "${events_path}" <<EOF2
wait_log fd 45000 BAM rx bundle: seq_id=${seq_one}
set_bam --disable
wait_log bam 45000 scheduler stream closed by validator conn=1
set_bam --enable
wait_log_count bam 45000 2 InitSchedulerStream:
wait_log bam 45000 scheduler<-validator auth proof .* conn=2
sleep_ms 2000
EOF2
      ;;
    quarantine_url_churn_queue_inflight)
      cat > "${events_path}" <<EOF2
wait_log fd 45000 BAM rx bundle: seq_id=${seq_one}
set_bam --url ${BAM_BAD_URL}
wait_log bam 45000 scheduler stream closed by validator conn=1
wait_log fd 45000 Connecting to ${BAM_BAD_URL_RE}
set_bam --url ${BAM_URL}
wait_log_count bam 45000 2 InitSchedulerStream:
wait_log bam 45000 scheduler<-validator auth proof .* conn=2
sleep_ms 2000
EOF2
      ;;
    disable_enable_tpu_release)
      cat > "${events_path}" <<EOF2
wait_log bam 30000 scheduler<-validator batch_result seq_id=${seq_one} status=committed
set_bam --disable
wait_log bam 45000 scheduler stream closed by validator conn=1
sleep_ms ${BAM_DISABLED_HOLD_MS}
set_bam --enable
wait_log_count bam 45000 2 InitSchedulerStream:
wait_log bam 45000 scheduler<-validator auth proof
EOF2
      ;;
    url_churn_unique_after_reconnect)
      cat > "${events_path}" <<EOF2
wait_log bam 30000 scheduler<-validator batch_result seq_id=${seq_one} status=committed
set_bam --url ${BAM_BAD_URL}
wait_log bam 45000 scheduler stream closed by validator conn=1
wait_log fd 45000 Connecting to ${BAM_BAD_URL_RE}
set_bam --url ${BAM_URL}
wait_log_count bam 45000 2 InitSchedulerStream:
wait_log bam 45000 scheduler<-validator auth proof
EOF2
      ;;
    bam_fee_url_churn_priority_commit|bam_fee_url_churn_same_slot_priority_commit)
      cat > "${events_path}" <<EOF2
wait_log bam 30000 scheduler<-validator batch_result seq_id=${seq_one} status=committed
set_bam --url ${BAM_BAD_URL}
wait_log bam 45000 scheduler stream closed by validator conn=1
wait_log fd 45000 Connecting to ${BAM_BAD_URL_RE}
EOF2
      ;;
    url_sni_churn)
      cat > "${events_path}" <<EOF2
wait_log bam 30000 scheduler<-validator batch_result seq_id=${seq_one} status=committed
set_bam --url ${BAM_BAD_URL} --sni churn.invalid
wait_log bam 45000 scheduler stream closed by validator conn=1
wait_log fd 45000 Connecting to ${BAM_BAD_URL_RE}
set_bam --url ${BAM_URL} --sni churn.invalid
wait_log_count bam 45000 2 InitSchedulerStream:
wait_log bam 45000 scheduler<-validator auth proof .* conn=2
set_bam --sni 127.0.0.1
wait_log_count bam 45000 3 InitSchedulerStream:
wait_log bam 45000 scheduler<-validator auth proof .* conn=3
EOF2
      ;;
    *)
      return 1
      ;;
  esac
}

render_bam_fee_url_churn_second_scenario() {
  local scenario_path="$1"
  local packet_two="$2"
  local seq_two="$3"
  cat > "${scenario_path}" <<EOF2
description = "Second BAM URL for fee-config churn: send one priority-fee transaction after the validator reconnects to the alternate BAM URL."
heartbeat_interval_ms = 1000

[[events]]
type = "sleep"
ms = 1000

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

url_bind_addr() {
  local url="$1"
  url="${url#http://}"
  url="${url#https://}"
  printf '%s\n' "${url%%/*}"
}

start_bam_controller_at() {
  local iter_dir="$1"
  local name="$2"
  local bam_log="$3"
  local scenario_path="$4"
  local bind_addr="$5"
  local builder_pubkey="$6"
  local track_extra="${7:-0}"
  local omit_block_engine_config="${8:-0}"
  local pid_file="${iter_dir}/${name}.pid"
  local start_script="${iter_dir}/start-${name}.sh"
  rm -f "${pid_file}"

  local -a bam_cmd=(
    cargo run --quiet --manifest-path "${BAM_MANIFEST}" --bin bam-test-server --
    --scenario benign
    --scenario-file "${scenario_path}"
    --bind "${bind_addr}"
    --tpu-ip 127.0.0.1
    --tpu-port "${BAM_TPU_PORT}"
    --tpu-fwd-ip 127.0.0.1
    --tpu-fwd-port "${BAM_TPU_FWD_PORT}"
    --shred-ip 127.0.0.1
    --shred-port "${BAM_SHRED_PORT}"
    --builder-commission-pct "${BAM_BUILDER_COMMISSION_PCT}"
    --commission-bps "${BAM_COMMISSION_BPS}"
  )
  if [[ -n "${builder_pubkey}" ]]; then
    bam_cmd+=(--builder-pubkey "${builder_pubkey}")
  fi
  if [[ "${omit_block_engine_config}" == "1" ]]; then
    bam_cmd+=(--omit-block-engine-config)
  fi
  if [[ -n "${BAM_SERVER_TLS_CERT}" || -n "${BAM_SERVER_TLS_KEY}" ]]; then
    [[ -n "${BAM_SERVER_TLS_CERT}" && -n "${BAM_SERVER_TLS_KEY}" ]] \
      || die "BAM_SERVER_TLS_CERT and BAM_SERVER_TLS_KEY must be supplied together"
    bam_cmd+=(--tls-cert "${BAM_SERVER_TLS_CERT}" --tls-key "${BAM_SERVER_TLS_KEY}")
  fi

  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'set -euo pipefail'
    printf '%s\n\n' "IFS=\$'\\n\\t'"
    printf 'exec'
    for arg in "${bam_cmd[@]}"; do
      printf ' %q' "${arg}"
    done
    printf '\n'
  } > "${start_script}"
  chmod +x "${start_script}"

  "${start_script}" >"${bam_log}" 2>&1 &
  LAST_BAM_PID=$!
  printf '%s\n' "${LAST_BAM_PID}" > "${pid_file}"
  LAST_BAM_PID_FILE="${pid_file}"
  LAST_BAM_START_SCRIPT="${start_script}"
  if [[ "${track_extra}" == "1" ]]; then
    EXTRA_BAM_PIDS+=("${LAST_BAM_PID}")
  fi
}

start_bam_controller() {
  local iter_dir="$1"
  local bam_log="$2"
  local scenario_path="$3"
  start_bam_controller_at "${iter_dir}" "bam" "${bam_log}" "${scenario_path}" "${BAM_BIND}" "${CURRENT_BAM_BUILDER_PUBKEY}" 0 "${CURRENT_BAM_OMIT_BLOCK_ENGINE_CONFIG}"
  BAM_PID="${LAST_BAM_PID}"
  CURRENT_BAM_PID_FILE="${LAST_BAM_PID_FILE}"
  CURRENT_BAM_START_SCRIPT="${LAST_BAM_START_SCRIPT}"
}

stop_bam_controller() {
  if [[ -n "${BAM_PID}" ]]; then
    kill "${BAM_PID}" 2>/dev/null || true
    wait "${BAM_PID}" 2>/dev/null || true
    BAM_PID=""
  fi
  local extra_pid
  for extra_pid in "${EXTRA_BAM_PIDS[@]}"; do
    kill "${extra_pid}" 2>/dev/null || true
    wait "${extra_pid}" 2>/dev/null || true
  done
  EXTRA_BAM_PIDS=()
}

start_bam_shred_capture() {
  local iter_dir="$1"
  local output_path="$2"
  local ready_path="${iter_dir}/bam_shred_capture.ready"
  local log_path="${iter_dir}/bam_shred_capture.log"

  rm -f -- "${output_path}" "${ready_path}" "${log_path}"
  python3 "${BAM_SHRED_CAPTURE_BIN}" \
    --bind 127.0.0.1 \
    --port "${BAM_SHRED_PORT}" \
    --output "${output_path}" \
    --ready-file "${ready_path}" \
    >"${log_path}" 2>&1 &
  BAM_SHRED_CAPTURE_PID=$!

  local deadline=$((SECONDS + 10))
  while (( SECONDS < deadline )); do
    if [[ -s "${ready_path}" ]]; then
      if grep -q '^ready$' "${ready_path}"; then
        return 0
      fi
      break
    fi
    if ! kill -0 "${BAM_SHRED_CAPTURE_PID}" 2>/dev/null; then
      break
    fi
    sleep 1
  done

  echo "--- ${log_path} ---" >&2
  cat "${log_path}" >&2 2>/dev/null || true
  echo "--- ${ready_path} ---" >&2
  cat "${ready_path}" >&2 2>/dev/null || true
  die "BAM shred capture failed to bind 127.0.0.1:${BAM_SHRED_PORT}"
}

stop_bam_shred_capture() {
  local output_path="$1"

  if [[ -n "${BAM_SHRED_CAPTURE_PID}" ]]; then
    kill "${BAM_SHRED_CAPTURE_PID}" 2>/dev/null || true
    wait "${BAM_SHRED_CAPTURE_PID}" 2>/dev/null || true
    BAM_SHRED_CAPTURE_PID=""
  fi

  if [[ "${CHECK_BAM_SHRED}" != "1" ]]; then
    return 0
  fi
  [[ -f "${output_path}" ]] || die "BAM shred capture did not write ${output_path}"

  local packet_count
  packet_count=$(python3 - "${output_path}" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as f:
    print(int(json.load(f).get("packet_count", 0)))
PY
)
  [[ "${packet_count}" =~ ^[0-9]+$ ]] || die "invalid BAM shred packet count in ${output_path}: ${packet_count}"
  (( packet_count > 0 )) || die "BAM shred capture saw no UDP packets on 127.0.0.1:${BAM_SHRED_PORT}; see ${output_path}"
}

write_fullfd_bootstrap_scenario() {
  local scenario_path="$1"
  cat >"${scenario_path}" <<'EOF2'
description = "fullfd BAM scheduler bootstrap placeholder"
heartbeat_interval_ms = 2000

[[events]]
type = "sleep"
ms = 600000
EOF2
}

send_normal_tpu_packet_after_bam_commit() {
  local packet_path="$1"
  local bam_log="$2"
  local seq_one="$3"
  local normal_tpu_log="$4"

  wait_for_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed" "${TIMEOUT_SECS}" \
    || die "BAM commit for seq_id=${seq_one} did not arrive before normal TPU injection"

  local payload
  payload=$(tr -d '[:space:]' < "${packet_path}")
  [[ -n "${payload}" ]] || die "normal TPU packet payload is empty"

  {
    printf 'normal_tpu_dst=127.0.0.1:%s\n' "${BAM_TPU_PORT}"
    printf 'normal_tpu_packet_file=%s\n' "${packet_path}"
    printf 'normal_tpu_after_bam_seq=%s\n' "${seq_one}"
    printf '\n'
  } >"${normal_tpu_log}"

  if ! run_fddev_txn_covered \
      --config "${CONFIG}" \
      --count 1 \
      --no-ready \
      --dst-ip 127.0.0.1 \
      --dst-port "${BAM_TPU_PORT}" \
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
  local normal_tpu_log="$3"

  wait_for_pattern "${bam_log}" '^InitSchedulerStream:' "${TIMEOUT_SECS}" \
    || die "BAM scheduler stream did not open before normal TPU precommit injection"

  local payload
  payload=$(tr -d '[:space:]' < "${packet_path}")
  [[ -n "${payload}" ]] || die "normal TPU packet payload is empty"

  {
    printf 'normal_tpu_dst=127.0.0.1:%s\n' "${BAM_TPU_PORT}"
    printf 'normal_tpu_packet_file=%s\n' "${packet_path}"
    printf 'normal_tpu_trigger=precommit\n'
    printf '\n'
  } >"${normal_tpu_log}"

  if ! run_fddev_txn_covered \
      --config "${CONFIG}" \
      --count 1 \
      --no-ready \
      --dst-ip 127.0.0.1 \
      --dst-port "${BAM_TPU_PORT}" \
      --payload-base64-encoded "${payload}" \
      >>"${normal_tpu_log}" 2>&1; then
    echo "--- tail ${normal_tpu_log} ---" >&2
    tail -n 80 "${normal_tpu_log}" >&2 || true
    die "normal TPU precommit injection failed"
  fi
}

run_fddev_txn_covered() {
  if [[ -n "${LIVE_COVERAGE_DIR}" ]]; then
    "${SUDO[@]}" env \
      "LLVM_PROFILE_FILE=${LIVE_COVERAGE_DIR}/${TARGET_NAME}-${ITER_MODE}-txn-%m-%p.profraw" \
      "${FDDEV}" txn "$@"
  else
    "${SUDO[@]}" "${FDDEV}" txn "$@"
  fi
}

send_normal_tpu_packet_after_queue_close() {
  local packet_path="$1"
  local bam_log="$2"
  local normal_tpu_log="$3"

  wait_for_pattern "${bam_log}" 'scripted close_stream' "${TIMEOUT_SECS}" \
    || die "queue-burst scheduler stream did not close before normal TPU injection"

  local payload
  payload=$(tr -d '[:space:]' < "${packet_path}")
  [[ -n "${payload}" ]] || die "normal TPU packet payload is empty"

  {
    printf 'normal_tpu_dst=127.0.0.1:%s\n' "${BAM_TPU_PORT}"
    printf 'normal_tpu_packet_file=%s\n' "${packet_path}"
    printf 'normal_tpu_trigger=queue_reconnect\n'
    printf '\n'
  } >"${normal_tpu_log}"

  if ! run_fddev_txn_covered \
      --config "${CONFIG}" \
      --count 1 \
      --no-ready \
      --dst-ip 127.0.0.1 \
      --dst-port "${BAM_TPU_PORT}" \
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
  local operator_log="$3"
  local normal_tpu_log="$4"

  wait_for_pattern "${operator_log}" '^operator: set_bam --disable' "${TIMEOUT_SECS}" \
    || die "BAM disable did not start before normal TPU release injection"
  wait_for_pattern "${bam_log}" 'scheduler stream closed by validator conn=1' "${TIMEOUT_SECS}" \
    || die "BAM scheduler stream did not close before normal TPU release injection"

  local payload
  payload=$(tr -d '[:space:]' < "${packet_path}")
  [[ -n "${payload}" ]] || die "normal TPU packet payload is empty"

  {
    printf 'normal_tpu_dst=127.0.0.1:%s\n' "${BAM_TPU_PORT}"
    printf 'normal_tpu_packet_file=%s\n' "${packet_path}"
    printf 'normal_tpu_trigger=bam_disabled\n'
    printf 'normal_tpu_burst_count=%s\n' "${NORMAL_TPU_BURST_COUNT}"
    printf 'normal_tpu_expected_landed=true\n'
    printf '\n'
  } >"${normal_tpu_log}"

  if ! run_fddev_txn_covered \
      --config "${CONFIG}" \
      --count "${NORMAL_TPU_BURST_COUNT}" \
      --no-ready \
      --dst-ip 127.0.0.1 \
      --dst-port "${BAM_TPU_PORT}" \
      --payload-base64-encoded "${payload}" \
      >>"${normal_tpu_log}" 2>&1; then
    echo "--- tail ${normal_tpu_log} ---" >&2
    tail -n 80 "${normal_tpu_log}" >&2 || true
    die "normal TPU release injection failed"
  fi

  if (( NORMAL_TPU_MATRIX_COUNT > 0 )); then
    local matrix_log="${NORMAL_TPU_MATRIX_DIR}/send.log"
    local matrix_sent=0
    local matrix_packet matrix_payload
    : >"${matrix_log}"
    for matrix_packet in "${NORMAL_TPU_MATRIX_DIR}"/*.packet; do
      [[ -f "${matrix_packet}" ]] || continue
      matrix_payload=$(tr -d '[:space:]' <"${matrix_packet}")
      [[ -n "${matrix_payload}" ]] || die "empty normal TPU matrix packet ${matrix_packet}"
      {
        printf 'matrix_packet=%s\n' "${matrix_packet}"
        run_fddev_txn_covered \
          --config "${CONFIG}" \
          --count 1 \
          --no-ready \
          --dst-ip 127.0.0.1 \
          --dst-port "${BAM_TPU_PORT}" \
          --payload-base64-encoded "${matrix_payload}"
      } >>"${matrix_log}" 2>&1 || die "normal TPU matrix injection failed for ${matrix_packet}"
      matrix_sent=$((matrix_sent + 1))
    done
    (( matrix_sent == NORMAL_TPU_MATRIX_COUNT )) \
      || die "normal TPU matrix sent ${matrix_sent} packets, expected ${NORMAL_TPU_MATRIX_COUNT}"
  fi
}

run_validator_control_probe() {
  local iter_dir="$1"
  local label="$2"
  local max_secs="$3"
  local use_tty="$4"
  shift 4

  local profile="${LIVE_COVERAGE_DIR}/${TARGET_NAME}-${ITER_MODE}-control-${label}.profraw"
  local output="${iter_dir}/control-${label}.out"
  : >"${profile}"
  chmod a+rw "${profile}"
  local -a command=(
    env "LLVM_PROFILE_FILE=${profile}"
    "${FDDEV}" "$@" --config "${CONFIG}"
  )

  set +e
  if [[ "${use_tty}" == "true" ]]; then
    local command_q
    printf -v command_q "%q " "${command[@]}"
    "${SUDO[@]}" timeout --signal=TERM --kill-after=2s "${max_secs}s" \
      /usr/bin/script --quiet --return --command "${command_q}" /dev/null \
      >"${output}" 2>&1
  else
    "${SUDO[@]}" timeout --signal=TERM --kill-after=2s "${max_secs}s" \
      "${command[@]}" >"${output}" 2>&1
  fi
  local rc=$?
  set -e
  printf "control_probe=%s exit=%s output=%s\n" "${label}" "${rc}" "${output}" \
    >>"${iter_dir}/control-coverage.log"
}

run_validator_control_coverage() {
  local iter_dir="$1"
  [[ -n "${LIVE_COVERAGE_DIR}" ]] || return 0

  : >"${iter_dir}/control-coverage.log"
  run_validator_control_probe "${iter_dir}" help            5 false help
  run_validator_control_probe "${iter_dir}" version         5 false version
  run_validator_control_probe "${iter_dir}" get-bam         5 false get-bam
  run_validator_control_probe "${iter_dir}" metrics         8 false metrics
  run_validator_control_probe "${iter_dir}" mem             8 false mem
  run_validator_control_probe "${iter_dir}" mem-sort        8 false mem --sort
  run_validator_control_probe "${iter_dir}" ready           8 false ready
  run_validator_control_probe "${iter_dir}" netconf         8 false netconf
  run_validator_control_probe "${iter_dir}" keys-pubkey     5 false keys pubkey "${iter_dir}/broad-workload-payer.json"
  run_validator_control_probe "${iter_dir}" monitor         8 true  monitor --duration 1000000000 --dt-min 50000000 --dt-max 200000000 --seed 1

  if [[ "${RUNNER_KIND}" == "fullfd" ]]; then
    run_validator_control_probe "${iter_dir}" monitor-gossip  3 false monitor-gossip --compact
    run_validator_control_probe "${iter_dir}" metrics-record  3 false metrics-record --interval 0.1 tile_pid
    local -a full_parser_commands=(
      gossip metrics-record monitor-gossip repair snapshot-load tower
    )
    local subcommand
    for subcommand in "${full_parser_commands[@]}"; do
      run_validator_control_probe "${iter_dir}" "args-${subcommand}" 3 false "${subcommand}" --help
    done
  else
    run_validator_control_probe "${iter_dir}" get-identity 8 false get-identity
  fi
}

run_bam_disabled_workload() {
  local iter_dir="$1"
  local workload_log="${iter_dir}/bam-disabled-workload.log"

  [[ -n "${BAM_DISABLED_WORKLOAD_SCRIPT}" ]] || return 0

  local control_bin="${FDCTL_BIN}"
  if [[ "${RUNNER_KIND}" == "fullfd" ]]; then
    control_bin="${FDDEV}"
  fi
  [[ -x "${control_bin}" ]] || die "missing BAM control binary ${control_bin}"

  local close_pattern='scheduler stream closed by validator'
  local close_count
  close_count=$(grep -cE "${close_pattern}" "${BAM_LOG}" 2>/dev/null || true)
  {
    printf 'bam_disable_control=%s\n' "${control_bin}"
    printf 'bam_close_count_before=%s\n' "${close_count}"
  } >"${workload_log}"

  local -a control_env=( env )
  if [[ -n "${LIVE_COVERAGE_DIR}" ]]; then
    control_env+=( "LLVM_PROFILE_FILE=${LIVE_COVERAGE_DIR}/${TARGET_NAME}-${ITER_MODE}-set-bam-%p.profraw" )
  fi
  if ! "${SUDO[@]}" "${control_env[@]}" "${control_bin}" set-bam \
      --config "${CONFIG}" --disable >>"${workload_log}" 2>&1; then
    die "failed to disable BAM before post-result differential workload"
  fi
  wait_for_pattern_count \
    "${BAM_LOG}" "${close_pattern}" "$((close_count + 1))" "${TIMEOUT_SECS}" \
    || die "BAM scheduler stream did not close before post-result differential workload"
  sleep 1

  {
    printf 'workload_script=%s\n' "${BAM_DISABLED_WORKLOAD_SCRIPT}"
    printf 'workload_rpc_url=%s\n' "${RPC_URL}"
    printf 'workload_runner_kind=%s\n' "${RUNNER_KIND}"
    printf '\n'
  } >>"${workload_log}"

  local payer_keypair="${iter_dir}/broad-workload-payer.json"
  seeded_keypair_file "${FROM_GENESIS_ACCOUNT}" "${payer_keypair}" \
    >"${iter_dir}/broad-workload-payer.out"

  if ! env \
      RPC_URL="${RPC_URL}" \
      GUI_URL="${GUI_URL}" \
      METRICS_URL="${METRICS_URL}" \
      SOLANA_BIN_DIR="${SOLANA_BIN_DIR}" \
      PAYER_KEYPAIR="${payer_keypair}" \
      VOTE_KEYPAIR="${VOTE_KEYPAIR_PATH}" \
      RUNNER_KIND="${RUNNER_KIND}" \
      TPU_PORT="${BAM_TPU_PORT}" \
      WORKLOAD_DIR="${iter_dir}/broad-workload" \
      "${BAM_DISABLED_WORKLOAD_SCRIPT}" \
      >>"${workload_log}" 2>&1; then
    echo "--- tail ${workload_log} ---" >&2
    tail -n 120 "${workload_log}" >&2 || true
    die "BAM-disabled coverage workload failed"
  fi
  run_validator_control_coverage "${iter_dir}"
}

start_operator_runner() {
  local events_path="$1"
  local fd_log="$2"
  local bam_log="$3"
  local operator_log="$4"
  local control_bin="${FDCTL_BIN}"
  if [[ "${RUNNER_KIND}" == "fullfd" ]]; then
    control_bin="${FDDEV}"
  fi
  "${OPERATOR_RUNNER}" \
    --config "${CONFIG}" \
    --control-bin "${control_bin}" \
    --fd-bin "${FDDEV}" \
    --events "${events_path}" \
    --fd-log "${fd_log}" \
    --bam-log "${bam_log}" \
    --fd-pid "${FD_PID}" \
    --fd-pid-file "${CURRENT_FD_PID_FILE}" \
    --bam-pid "${BAM_PID}" \
    --bam-pid-file "${CURRENT_BAM_PID_FILE}" \
    --bam-start-script "${CURRENT_BAM_START_SCRIPT}" \
    >"${operator_log}" 2>&1 &
  OPERATOR_PID=$!
}


run_helper_mode() {
  local iter_mode="$1"
  local iter_dir="$2"
  local iter_seed="$3"
  local iter_input_family="$4"
  local lamports="$5"

  if [[ -n "${LIVE_COVERAGE_DIR}" ]]; then
    export VALIDATOR_PROFILE_PATTERN="${LIVE_COVERAGE_DIR}/${TARGET_NAME}-${iter_mode}-helper-%m-%p.profraw"
  fi

  if [[ "${RUNNER_KIND}" == "fullfd" ]]; then
    case "${iter_mode}" in
      fd_pause_resume_churn|bam_pause_resume_churn|fd_restart_churn|bam_restart_churn)
        ;;
      *)
        return 2
        ;;
    esac
  fi

  if [[ "${iter_mode}" == "seq_id_out_of_order_multi_batch" || "${iter_mode}" == "seq_id_wrap_out_of_order_multi_batch" || "${iter_mode}" == "seq_id_wrap_conflicting_spend_multi_batch" || "${iter_mode}" == "mixed_multi_batch" || "${iter_mode}" == "mixed_empty_multi_batch" || "${iter_mode}" == "mixed_malformed_multi_batch" || "${iter_mode}" == "mixed_bad_signature_multi_batch" || "${iter_mode}" == "mixed_bad_signature_reconnect" || "${iter_mode}" == "mixed_stale_multi_batch" || "${iter_mode}" == "mixed_stale_reconnect" || "${iter_mode}" == "mixed_terminal_producers_reconnect" || "${iter_mode}" == "random_mixed_multi_batch" || "${iter_mode}" == "vote_reject_once" || "${iter_mode}" == "vote_reject_reconnect" ]]; then
    return 2
  fi

  case "${iter_mode}" in
    atomic_revert)
      if ! USE_SUDO="${USE_SUDO}" \
          BAM_BIND="${BAM_BIND}" \
          BAM_URL="${BAM_URL}" \
          BAM_TPU_PORT="${BAM_TPU_PORT}" \
          BAM_TPU_FWD_PORT="${BAM_TPU_FWD_PORT}" \
          BAM_SHRED_PORT="${BAM_SHRED_PORT}" \
          "${ATOMIC_REVERT_RUNNER}" \
          --config "${CONFIG}" \
          --rpc-url "${RPC_URL}" \
          --log-dir "${iter_dir}"; then
        return 1
      fi
      cat > "${iter_dir}/summary.txt" <<EOF2
seed=${iter_seed}
mode=${iter_mode}
input_family=${iter_input_family}
helper_runner=${ATOMIC_REVERT_RUNNER}
scenario_file=${iter_dir}/scenario.toml
input_note=atomic_revert_on_error=true
EOF2
	      ;;
	    partial_drain_reconnect)
      local batch_count=15
      if ! USE_SUDO="${USE_SUDO}" \
          BAM_BIND="${BAM_BIND}" \
          BAM_URL="${BAM_URL}" \
          BAM_TPU_PORT="${BAM_TPU_PORT}" \
          BAM_TPU_FWD_PORT="${BAM_TPU_FWD_PORT}" \
          BAM_SHRED_PORT="${BAM_SHRED_PORT}" \
          "${PARTIAL_DRAIN_RUNNER}" \
          --config "${CONFIG}" \
          --rpc-url "${RPC_URL}" \
          --log-dir "${iter_dir}" \
          --batch-count "${batch_count}" \
          --lamports "${lamports}" \
          --from-genesis-account "${FROM_GENESIS_ACCOUNT}" \
          --first-to-genesis-account "${TO_GENESIS_ACCOUNT_ONE}"; then
        return 1
      fi
      cat > "${iter_dir}/summary.txt" <<EOF2
seed=${iter_seed}
mode=${iter_mode}
input_family=${iter_input_family}
helper_runner=${PARTIAL_DRAIN_RUNNER}
scenario_file=${iter_dir}/scenario.toml
input_path=${iter_dir}/input.txnctx
input_note=batch_count=${batch_count} lamports=${lamports}
EOF2
	      ;;
	    queue_burst_reconnect|queue_burst64_reconnect|queue_burst64_leader_plus1_reconnect|schedule_boundary_jitter|queue_reconnect_timing_jitter|queue_burst128_reconnect|queue_burst256_reconnect|queue_burst512_reconnect|queue_burst_leader_reconnect|queue_burst64_leader_reconnect)
		      local batch_count=32
		      local max_schedule_slot=max
		      local close_after_results=1
		      case "${iter_mode}" in
	        queue_burst64_reconnect)
	          batch_count=64
	          ;;
	        queue_burst64_leader_plus1_reconnect)
	          batch_count=64
	          max_schedule_slot=leader+1
	          ;;
		        schedule_boundary_jitter)
		          local jitter_batch_counts=(16 32 64 96 128)
		          local jitter_max_schedule_slots=(leader leader+1 leader+2)
		          batch_count="${jitter_batch_counts[$(( iter_seed % ${#jitter_batch_counts[@]} ))]}"
		          max_schedule_slot="${jitter_max_schedule_slots[$(( (iter_seed / ${#jitter_batch_counts[@]}) % ${#jitter_max_schedule_slots[@]} ))]}"
		          ;;
			        queue_reconnect_timing_jitter)
			          local timing_batch_counts=(8 16 32 64)
			          local timing_max_schedule_slots=(leader leader+1 leader+2 max)
			          batch_count="${timing_batch_counts[$(( iter_seed % ${#timing_batch_counts[@]} ))]}"
		          max_schedule_slot="${timing_max_schedule_slots[$(( (iter_seed / ${#timing_batch_counts[@]}) % ${#timing_max_schedule_slots[@]} ))]}"
		          case $(( (iter_seed / (${#timing_batch_counts[@]} * ${#timing_max_schedule_slots[@]})) % 5 )) in
		            0) close_after_results=0 ;;
		            1) close_after_results=1 ;;
		            2) close_after_results=$(( batch_count / 4 )) ;;
		            3) close_after_results=$(( batch_count / 2 )) ;;
			            4) close_after_results=$(( batch_count - 1 )) ;;
			          esac
			          ;;
		        queue_burst128_reconnect)
	          batch_count=128
	          ;;
	        queue_burst256_reconnect)
	          batch_count=256
	          ;;
	        queue_burst512_reconnect)
	          batch_count=512
	          ;;
	        queue_burst_leader_reconnect)
	          max_schedule_slot=leader
	          ;;
	        queue_burst64_leader_reconnect)
	          batch_count=64
	          max_schedule_slot=leader
	          ;;
	      esac
	      if [[ -n "${QUEUE_BURST_BATCH_COUNT}" ]]; then
	        batch_count="${QUEUE_BURST_BATCH_COUNT}"
	      fi
		      if [[ -n "${QUEUE_BURST_MAX_SCHEDULE_SLOT}" ]]; then
		        max_schedule_slot="${QUEUE_BURST_MAX_SCHEDULE_SLOT}"
		      fi
		      if [[ -n "${QUEUE_BURST_CLOSE_AFTER_RESULTS}" ]]; then
		        close_after_results="${QUEUE_BURST_CLOSE_AFTER_RESULTS}"
		      fi
	      local burst_lamports="${lamports}"
	      if [[ "${burst_lamports}" =~ ^[0-9]+$ ]] && (( burst_lamports < 1000000 )); then
	        burst_lamports=1000000
	      fi
	      if ! USE_SUDO="${USE_SUDO}" \
	          BAM_BIND="${BAM_BIND}" \
	          BAM_URL="${BAM_URL}" \
	          BAM_TPU_PORT="${BAM_TPU_PORT}" \
	          BAM_TPU_FWD_PORT="${BAM_TPU_FWD_PORT}" \
	          BAM_SHRED_PORT="${BAM_SHRED_PORT}" \
	          "${PARTIAL_DRAIN_RUNNER}" \
	          --config "${CONFIG}" \
	          --rpc-url "${RPC_URL}" \
	          --log-dir "${iter_dir}" \
		          --batch-count "${batch_count}" \
		          --lamports "${burst_lamports}" \
		          --max-schedule-slot "${max_schedule_slot}" \
		          --close-after-results "${close_after_results}" \
		          --from-genesis-account "${FROM_GENESIS_ACCOUNT}" \
	          --first-to-genesis-account "${TO_GENESIS_ACCOUNT_ONE}"; then
	        return 1
	      fi
	      cat > "${iter_dir}/summary.txt" <<EOF2
seed=${iter_seed}
mode=${iter_mode}
input_family=${iter_input_family}
helper_runner=${PARTIAL_DRAIN_RUNNER}
scenario_file=${iter_dir}/scenario.toml
input_path=${iter_dir}/input.txnctx
	input_note=batch_count=${batch_count} lamports=${burst_lamports} queue_burst=true max_schedule_slot=${max_schedule_slot} close_after_results=${close_after_results}
EOF2
	      ;;
	    duplicate_seq_split)
      local seq_id=$((500 + iter_seed))
      if ! USE_SUDO="${USE_SUDO}" \
          BAM_BIND="${BAM_BIND}" \
          BAM_URL="${BAM_URL}" \
          BAM_TPU_PORT="${BAM_TPU_PORT}" \
          BAM_TPU_FWD_PORT="${BAM_TPU_FWD_PORT}" \
          BAM_SHRED_PORT="${BAM_SHRED_PORT}" \
          "${DUPLICATE_SEQ_SPLIT_RUNNER}" \
          --config "${CONFIG}" \
          --rpc-url "${RPC_URL}" \
          --log-dir "${iter_dir}" \
          --base-lamports "${lamports}" \
          --from-genesis-account "${FROM_GENESIS_ACCOUNT}" \
          --to-genesis-account "${TO_GENESIS_ACCOUNT_ONE}" \
          --seq-id "${seq_id}"; then
        return 1
      fi
      cat > "${iter_dir}/summary.txt" <<EOF2
seed=${iter_seed}
mode=${iter_mode}
input_family=${iter_input_family}
helper_runner=${DUPLICATE_SEQ_SPLIT_RUNNER}
scenario_file=${iter_dir}/scenario.toml
input_note=base_lamports=${lamports} seq_id=${seq_id}
EOF2
      ;;
    fd_pause_resume_churn)
      local control_bin="${FDCTL_BIN}"
      if [[ "${RUNNER_KIND}" == "fullfd" ]]; then
        control_bin="${FDDEV}"
      fi
      if ! USE_SUDO="${USE_SUDO}" "${FD_PAUSE_RESUME_RUNNER}" \
          --config "${CONFIG}" \
          --fd-bin "${FDDEV}" \
          --control-bin "${control_bin}" \
          --timeout-secs "${TIMEOUT_SECS}" \
          -- --log-dir "${iter_dir}" \
          --bam-bind "${BAM_BIND}" \
          --bam-tpu-port "${BAM_TPU_PORT}" \
          --bam-tpu-fwd-port "${BAM_TPU_FWD_PORT}" \
          --bam-shred-port "${BAM_SHRED_PORT}"; then
        return 1
      fi
      cat > "${iter_dir}/summary.txt" <<EOF2
seed=${iter_seed}
mode=${iter_mode}
input_family=${iter_input_family}
helper_runner=${FD_PAUSE_RESUME_RUNNER}
input_note=timeout_secs=${TIMEOUT_SECS}
EOF2
      ;;
    bam_pause_resume_churn)
      local control_bin="${FDCTL_BIN}"
      if [[ "${RUNNER_KIND}" == "fullfd" ]]; then
        control_bin="${FDDEV}"
      fi
      if ! USE_SUDO="${USE_SUDO}" "${BAM_PAUSE_RESUME_RUNNER}" \
          --config "${CONFIG}" \
          --fd-bin "${FDDEV}" \
          --control-bin "${control_bin}" \
          --timeout-secs "${TIMEOUT_SECS}" \
          -- --log-dir "${iter_dir}" \
          --bam-bind "${BAM_BIND}" \
          --bam-tpu-port "${BAM_TPU_PORT}" \
          --bam-tpu-fwd-port "${BAM_TPU_FWD_PORT}" \
          --bam-shred-port "${BAM_SHRED_PORT}"; then
        return 1
      fi
      cat > "${iter_dir}/summary.txt" <<EOF2
seed=${iter_seed}
mode=${iter_mode}
input_family=${iter_input_family}
helper_runner=${BAM_PAUSE_RESUME_RUNNER}
input_note=timeout_secs=${TIMEOUT_SECS}
EOF2
      ;;
    fd_restart_churn)
      local control_bin="${FDCTL_BIN}"
      if [[ "${RUNNER_KIND}" == "fullfd" ]]; then
        control_bin="${FDDEV}"
      fi
      if ! USE_SUDO="${USE_SUDO}" "${FD_RESTART_RUNNER}" \
          --config "${CONFIG}" \
          --fd-bin "${FDDEV}" \
          --control-bin "${control_bin}" \
          --timeout-secs "${TIMEOUT_SECS}" \
          -- --log-dir "${iter_dir}" \
          --bam-bind "${BAM_BIND}" \
          --bam-tpu-port "${BAM_TPU_PORT}" \
          --bam-tpu-fwd-port "${BAM_TPU_FWD_PORT}" \
          --bam-shred-port "${BAM_SHRED_PORT}"; then
        return 1
      fi
      cat > "${iter_dir}/summary.txt" <<EOF2
seed=${iter_seed}
mode=${iter_mode}
input_family=${iter_input_family}
helper_runner=${FD_RESTART_RUNNER}
input_note=timeout_secs=${TIMEOUT_SECS}
EOF2
      ;;
    bam_restart_churn)
      local control_bin="${FDCTL_BIN}"
      if [[ "${RUNNER_KIND}" == "fullfd" ]]; then
        control_bin="${FDDEV}"
      fi
      if ! USE_SUDO="${USE_SUDO}" "${BAM_RESTART_RUNNER}" \
          --config "${CONFIG}" \
          --fd-bin "${FDDEV}" \
          --control-bin "${control_bin}" \
          --timeout-secs "${TIMEOUT_SECS}" \
          -- --log-dir "${iter_dir}" \
          --bam-bind "${BAM_BIND}" \
          --bam-tpu-port "${BAM_TPU_PORT}" \
          --bam-tpu-fwd-port "${BAM_TPU_FWD_PORT}" \
          --bam-shred-port "${BAM_SHRED_PORT}"; then
        return 1
      fi
      cat > "${iter_dir}/summary.txt" <<EOF2
seed=${iter_seed}
mode=${iter_mode}
input_family=${iter_input_family}
helper_runner=${BAM_RESTART_RUNNER}
input_note=timeout_secs=${TIMEOUT_SECS}
EOF2
      ;;
    url_sni_churn)
      if ! USE_SUDO="${USE_SUDO}" "${URL_SNI_CHURN_RUNNER}" \
          --config "${CONFIG}" \
          --timeout-secs "${TIMEOUT_SECS}" \
          -- --log-dir "${iter_dir}"; then
        return 1
      fi
      cat > "${iter_dir}/summary.txt" <<EOF2
seed=${iter_seed}
mode=${iter_mode}
input_family=${iter_input_family}
helper_runner=${URL_SNI_CHURN_RUNNER}
input_note=timeout_secs=${TIMEOUT_SECS}
EOF2
      ;;
    *)
      return 2
      ;;
  esac

  write_normalized_outcome "${iter_dir}" "${iter_mode}" "${iter_input_family}" "helper"
  return 0
}

validate_random_mixed_results() {
  local pattern="$1"
  local seq_base="$2"
  local fd_log="$3"
  local bam_log="$4"

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
        require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_id}" "validator decoded random-mixed ${token} batch seq_id=${seq_id}" "${TIMEOUT_SECS}"
        require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_id} status=committed txns=1 conn=1" "random-mixed ${token} committed seq_id=${seq_id}" "${TIMEOUT_SECS}"
        ;;
      replay1|replay2)
        require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_id}" "validator decoded random-mixed ${token} batch seq_id=${seq_id}" "${TIMEOUT_SECS}"
        require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_id} status=not_committed reason=transaction_error index=0 detail=ALREADY_PROCESSED conn=1" "random-mixed ${token} rejected as ALREADY_PROCESSED seq_id=${seq_id}" "${TIMEOUT_SECS}"
        ;;
      empty)
        require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_id} status=not_committed reason=deserialization_error index=0 detail=EMPTY conn=1" "random-mixed empty batch rejected seq_id=${seq_id}" "${TIMEOUT_SECS}"
        ;;
      stale1|stale2)
        require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_id} status=not_committed reason=scheduling_error detail=OUTSIDE_LEADER_SLOT conn=1" "random-mixed stale batch rejected seq_id=${seq_id}" "${TIMEOUT_SECS}"
        ;;
      *)
        die "unsupported random mixed token ${token}"
        ;;
    esac
    idx=$((idx + 1))
  done

  require_pattern "${bam_log}" "scripted wait_inbound satisfied conn=1 kind=committed_batch observed=${committed_count}" 'scenario observed expected committed random-mixed batch count' "${TIMEOUT_SECS}"
  require_pattern "${bam_log}" "scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=${not_committed_count}" 'scenario observed expected not_committed random-mixed batch count' "${TIMEOUT_SECS}"
}

validate_iteration() {
  local iter_mode="$1"
  local seq_one="$2"
  local seq_two="$3"
  local fd_log="$4"
  local bam_log="$5"
  local operator_log="$6"
  local to_one="$7"
  local initial_to_one="$8"
  local expected_to_one="$9"
  local to_two="${10}"
  local initial_to_two="${11}"
  local expected_to_two="${12}"

  echo "  checking common BAM handshake and decode evidence"
  require_pattern "${bam_log}" '^GetAuthChallenge:' 'validator requested BAM auth challenge' "${TIMEOUT_SECS}"
  require_pattern "${bam_log}" '^GetBuilderConfig:' 'validator requested BAM config' "${TIMEOUT_SECS}"
  require_pattern "${bam_log}" '^InitSchedulerStream:' 'validator opened a scheduler stream' "${TIMEOUT_SECS}"
  require_pattern "${bam_log}" 'scheduler<-validator auth proof' 'validator sent scheduler auth proof' "${TIMEOUT_SECS}"
  require_pattern "${fd_log}" 'BAM identity pubkey updated to' 'validator initialized BAM identity' "${TIMEOUT_SECS}"
  if [[ "${RUNNER_KIND}" == "fullfd" ]]; then
    require_pattern "${fd_log}" 'Publishing BAM shred receivers active=1' 'validator applied BAM scheduler config' "${TIMEOUT_SECS}"
  else
    require_pattern "${fd_log}" 'Updated TPU addresses:' 'validator applied BAM TPU config' "${TIMEOUT_SECS}"
  fi

  case "${iter_mode}" in
    generated_bundle)
      [[ "${EXPECTED_TERMINAL_RESULTS}" =~ ^[0-9]+$ && "${EXPECTED_TERMINAL_RESULTS}" -ge 1 ]] \
        || die "generated bundle did not record a positive terminal-result count"
      require_pattern "${bam_log}" 'scripted send_multi_batch seq_ids=\[' 'scenario emitted the generated multi-batch request' "${TIMEOUT_SECS}"
      ;;
    mixed_multi_batch)
      local seq_mid=$((seq_one + 1))
      require_pattern "${bam_log}" "scripted send_multi_batch seq_ids=\\[${seq_one}, ${seq_mid}, ${seq_two}\\]" 'scenario emitted the mixed multi-batch request' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_mid}" 'validator decoded the replayed BAM bundle inside the multi-batch message' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the second unique BAM bundle inside the multi-batch message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first BAM packet committed inside the multi-batch message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_mid} status=not_committed .* conn=1" 'replayed BAM packet was rejected inside the multi-batch message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=1" 'second BAM packet committed inside the multi-batch message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=committed_batch observed=2' 'scenario observed two committed batches on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed one not_committed batch on conn=1' "${TIMEOUT_SECS}"
      ;;
    duplicate_seq_split)
      require_pattern "${bam_log}" "scripted send_split_batch seq_id=${seq_one} splits=\\[5, 1\\]" 'scenario emitted the duplicate-seq split request' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=5 mode=atomic" 'validator decoded the first duplicate-seq split fragment' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=1 mode=atomic" 'validator decoded the second duplicate-seq split fragment' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=5 conn=1" 'first duplicate-seq split fragment committed on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'second duplicate-seq split fragment committed on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=batch_result observed=2' 'scenario observed two duplicate-seq split results on conn=1' "${TIMEOUT_SECS}"
      ;;
    duplicate_seq_split_reconnect)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=5 mode=atomic" 'validator decoded the first duplicate-seq reconnect fragment' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=5 conn=1" 'first duplicate-seq reconnect fragment committed on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'duplicate-seq reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for duplicate-seq split reconnect' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'duplicate-seq split reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=1 mode=atomic" 'validator decoded the second duplicate-seq reconnect fragment' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=2" 'second duplicate-seq reconnect fragment committed on conn=2' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 2 'duplicate-seq reconnect produced exactly two terminal BAM results' "${TIMEOUT_SECS}"
      ;;
    seq_id_wrap_conflicting_spend_multi_batch)
      require_pattern "${bam_log}" "scripted send_multi_batch seq_ids=\\[${seq_two}, ${seq_one}\\] packet_counts=\\[1, 1\\]" 'scenario emitted the wrap-boundary conflicting-spend request' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the lower-seq conflicting batch inside the wrap-boundary message' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the higher-seq conflicting batch inside the wrap-boundary message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=1" 'higher-seq conflicting batch committed on Frankendancer' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'lower-seq conflicting batch returned as a non-revert committed batch on Frankendancer' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result_tx seq_id=${seq_one} tx_index=0 execution_success=false" 'lower-seq conflicting transaction failed after Frankendancer spent the payer balance first' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=batch_result observed=2' 'scenario observed two terminal conflicting-spend batch results on conn=1' "${TIMEOUT_SECS}"
      ;;
    seq_id_out_of_order_multi_batch|seq_id_wrap_out_of_order_multi_batch)
      require_pattern "${bam_log}" "scripted send_multi_batch seq_ids=\\[${seq_two}, ${seq_one}\\] packet_counts=\\[1, 1\\]" 'scenario emitted the descending-seq multi-batch request' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the lower-seq BAM batch inside the descending-seq message' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the higher-seq BAM batch inside the descending-seq message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'lower-seq BAM packet committed inside the descending-seq message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=1" 'higher-seq BAM packet committed inside the descending-seq message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=committed_batch observed=2' 'scenario observed two committed descending-seq batches on conn=1' "${TIMEOUT_SECS}"
      ;;
	    mixed_empty_multi_batch)
	      local seq_mid=$((seq_one + 1))
	      require_pattern "${bam_log}" "scripted send_multi_batch seq_ids=\\[${seq_one}, ${seq_mid}, ${seq_two}\\] packet_counts=\\[1, 0, 1\\]" 'scenario emitted the valid-empty-valid multi-batch request' "${TIMEOUT_SECS}"
	      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the first valid BAM bundle inside the multi-batch message' "${TIMEOUT_SECS}"
	      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the second valid BAM bundle inside the multi-batch message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first BAM packet committed inside the valid-empty-valid message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_mid} status=not_committed reason=deserialization_error index=0 detail=EMPTY conn=1" 'empty middle batch was rejected inside the multi-batch message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=1" 'second BAM packet committed inside the valid-empty-valid message' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=committed_batch observed=2' 'scenario observed two committed batches on conn=1' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed one empty-batch rejection on conn=1' "${TIMEOUT_SECS}"
	      ;;
	    mixed_malformed_multi_batch)
	      local seq_mid=$((seq_one + 1))
	      require_pattern "${bam_log}" "scripted send_multi_batch seq_ids=\\[${seq_one}, ${seq_mid}, ${seq_two}\\] packet_counts=\\[1, 1, 1\\]" 'scenario emitted the valid-malformed-valid multi-batch request' "${TIMEOUT_SECS}"
	      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the first valid BAM bundle inside the malformed-isolation message' "${TIMEOUT_SECS}"
	      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the second valid BAM bundle inside the malformed-isolation message' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first BAM packet committed inside the valid-malformed-valid message' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_mid} status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR conn=1" 'malformed middle batch was rejected inside the multi-batch message' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=1" 'second BAM packet committed inside the valid-malformed-valid message' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=committed_batch observed=2' 'scenario observed two committed batches on conn=1' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed one malformed-batch rejection on conn=1' "${TIMEOUT_SECS}"
	      ;;
	    mixed_bad_signature_multi_batch)
	      local seq_mid=$((seq_one + 1))
	      require_pattern "${bam_log}" "scripted send_multi_batch seq_ids=\\[${seq_one}, ${seq_mid}, ${seq_two}\\] packet_counts=\\[1, 1, 1\\]" 'scenario emitted the valid-bad-signature-valid multi-batch request' "${TIMEOUT_SECS}"
	      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the first valid BAM bundle inside the bad-signature-isolation message' "${TIMEOUT_SECS}"
	      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the second valid BAM bundle inside the bad-signature-isolation message' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first BAM packet committed inside the valid-bad-signature-valid message' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_mid} status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR conn=1" 'bad-signature middle batch was rejected inside the multi-batch message' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=1" 'second BAM packet committed inside the valid-bad-signature-valid message' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=committed_batch observed=2' 'scenario observed two committed batches on conn=1' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed one bad-signature-batch rejection on conn=1' "${TIMEOUT_SECS}"
	      ;;
    mixed_bad_signature_reconnect)
      local seq_mid=$((seq_one + 1))
      require_pattern "${bam_log}" "scripted send_multi_batch seq_ids=\\[${seq_one}, ${seq_mid}, ${seq_two}\\] packet_counts=\\[1, 1, 1\\]" 'scenario emitted the valid-bad-signature-valid reconnect multi-batch request' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the first valid BAM bundle before bad-signature reconnect' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the second valid BAM bundle before bad-signature reconnect' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_mid} status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR conn=1" 'bad-signature middle batch was rejected before reconnect' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'mixed bad-signature reconnect scenario closed conn=1 after rejection' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected after mixed bad-signature stream close' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 replay disabled; not replaying' 'mixed bad-signature reconnect kept replay disabled on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=[12]" 'first valid BAM packet committed across mixed bad-signature reconnect' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=[12]" 'second valid BAM packet committed across mixed bad-signature reconnect' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=(${seq_one}|${seq_mid}|${seq_two}) status=" 3 'mixed bad-signature reconnect produced exactly three terminal BAM results' "${TIMEOUT_SECS}"
      ;;
    mixed_stale_multi_batch)
      local seq_mid=$((seq_one + 1))
      require_pattern "${bam_log}" "scripted send_multi_batch seq_ids=\\[${seq_one}, ${seq_mid}, ${seq_two}\\] packet_counts=\\[1, 1, 1\\]" 'scenario emitted the valid-stale-valid multi-batch request' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the first valid BAM bundle inside the multi-batch message' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the second valid BAM bundle inside the multi-batch message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first BAM packet committed inside the valid-stale-valid message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_mid} status=not_committed reason=scheduling_error detail=OUTSIDE_LEADER_SLOT conn=1" 'stale middle batch was rejected inside the multi-batch message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=1" 'second BAM packet committed inside the valid-stale-valid message' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=committed_batch observed=2' 'scenario observed two committed batches on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed one stale-batch rejection on conn=1' "${TIMEOUT_SECS}"
      ;;
    mixed_stale_reconnect)
      local seq_mid=$((seq_one + 1))
      require_pattern "${bam_log}" "scripted send_multi_batch seq_ids=\\[${seq_one}, ${seq_mid}, ${seq_two}\\] packet_counts=\\[1, 1, 1\\]" 'scenario emitted the valid-stale-valid reconnect multi-batch request' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the first valid BAM bundle before stale reconnect' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the second valid BAM bundle before stale reconnect' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_mid} status=not_committed reason=scheduling_error detail=OUTSIDE_LEADER_SLOT conn=1" 'stale middle batch was rejected before reconnect' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'mixed stale reconnect scenario closed conn=1 after stale rejection' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected after mixed stale stream close' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 replay disabled; not replaying' 'mixed stale reconnect kept replay disabled on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=[12]" 'first valid BAM packet committed across mixed stale reconnect' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=[12]" 'second valid BAM packet committed across mixed stale reconnect' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=(${seq_one}|${seq_mid}|${seq_two}) status=" 3 'mixed stale reconnect produced exactly three terminal BAM results' "${TIMEOUT_SECS}"
      ;;
    mixed_terminal_producers_reconnect)
      local seq_malformed=$((seq_one + 1))
      local seq_stale=$((seq_one + 2))
      local seq_bad_sig=$((seq_one + 3))
      require_pattern "${bam_log}" "scripted send_multi_batch seq_ids=\\[${seq_one}, ${seq_malformed}, ${seq_stale}, ${seq_bad_sig}, ${seq_two}\\] packet_counts=\\[1, 1, 1, 1, 1\\]" 'scenario emitted the mixed terminal-producer reconnect multi-batch request' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the first valid BAM bundle before mixed terminal-producer reconnect' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the second valid BAM bundle before mixed terminal-producer reconnect' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'mixed terminal-producer reconnect scenario closed conn=1 after a rejection' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected after mixed terminal-producer stream close' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 replay disabled; not replaying' 'mixed terminal-producer reconnect kept replay disabled on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=[12]" 'first valid BAM packet committed across mixed terminal-producer reconnect' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_malformed} status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR conn=[12]" 'malformed batch produced one sanitize rejection across mixed terminal-producer reconnect' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_stale} status=not_committed reason=scheduling_error detail=OUTSIDE_LEADER_SLOT conn=[12]" 'stale batch produced one scheduling rejection across mixed terminal-producer reconnect' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_bad_sig} status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR conn=[12]" 'bad-signature batch produced one sanitize rejection across mixed terminal-producer reconnect' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=[12]" 'second valid BAM packet committed across mixed terminal-producer reconnect' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=(${seq_one}|${seq_malformed}|${seq_stale}|${seq_bad_sig}|${seq_two}) status=" 5 'mixed terminal-producer reconnect produced exactly five terminal BAM results' "${TIMEOUT_SECS}"
      ;;
    random_mixed_multi_batch)
      validate_random_mixed_results "${ITER_RANDOM_MIX_PATTERN}" "${seq_one}" "${fd_log}" "${bam_log}"
      ;;
    vote_reject_once|vote_reject_reconnect|raw_kunorpus_once|raw_kunorpus_reconnect|stale_slot_reject|stale_slot_reject_reconnect|empty_batch_reject|empty_batch_reject_reconnect|malformed_first_atomic|malformed_first_atomic_reconnect|malformed_tail_atomic|malformed_tail_atomic_reconnect|bad_signature_first_atomic|bad_signature_first_atomic_reconnect|bad_signature_tail_atomic|bad_signature_tail_atomic_reconnect|non_atomic_inconsistent_bundle|non_atomic_first_overdraft|non_atomic_mid_overdraft|non_atomic_partial_overdraft|non_atomic_partial_overdraft_reconnect|non_atomic_partial_resolver_fail|non_atomic_partial_blockhash_fail|non_atomic_partial_duplicate_sig|non_atomic_partial_cu_fail|non_atomic_valid_multi_packet)
      ;;
    valid_alt_commit)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the valid-ALT v0 transfer batch' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'valid-ALT v0 transfer committed on conn=1' "${TIMEOUT_SECS}"
      ;;
    invalid_alt_missing_table|invalid_alt_missing_table_reconnect)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the invalid-ALT v0 transfer batch' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR" 'invalid-ALT missing-table batch rejected with SANITIZE_ERROR' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'invalid-ALT missing-table mode produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      if [[ "${iter_mode}" == "invalid_alt_missing_table_reconnect" ]]; then
        require_pattern "${bam_log}" 'scripted close_stream' 'invalid-ALT reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
        require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected after invalid-ALT scheduler close' "${TIMEOUT_SECS}"
        require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'invalid-ALT reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      else
        require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 invalid-ALT rejection' "${TIMEOUT_SECS}"
      fi
      ;;
    external_scenario)
      if scenario_has_send_events "${SCENARIO_PATH}"; then
        local expected_external_results=1
        if [[ "${EXPECTED_TERMINAL_RESULTS:-}" =~ ^[0-9]+$ ]]; then
          expected_external_results="${EXPECTED_TERMINAL_RESULTS}"
        elif [[ "${MATERIALIZED_PACKET_SETS:-}" =~ ^[0-9]+$ && "${MATERIALIZED_PACKET_SETS}" -gt 0 ]]; then
          expected_external_results="${MATERIALIZED_PACKET_SETS}"
        fi
        require_pattern "${bam_log}" 'scripted send_(batch|batch_flood|multi_batch|split_batch)' 'external scenario emitted at least one live BAM batch' "${TIMEOUT_SECS}"
        require_pattern_count "${bam_log}" 'scheduler<-validator batch_result seq_id=' "${expected_external_results}" "validator returned ${expected_external_results} terminal BAM result(s) for the external scenario" "${TIMEOUT_SECS}"
      else
        echo "  external scenario has no live BAM send events"
      fi
      ;;
    *)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the first BAM bundle' "${TIMEOUT_SECS}"
      ;;
  esac

  case "${iter_mode}" in
	    commit_once|seq_id_max_once|bam_fee_priority_commit|fee_only_commit|durable_nonce_commit)
	      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first BAM transaction committed on conn=1' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=committed_batch observed=1' 'scenario observed conn=1 committed batch' "${TIMEOUT_SECS}"
	      ;;
    fee_only_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'fee-only reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for fee-only result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'fee-only reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1" 'fee-only reconnect transaction committed' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'fee-only reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    durable_nonce_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'durable-nonce reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for durable-nonce result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'durable-nonce reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1" 'durable-nonce reconnect transaction committed' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'durable-nonce reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    durable_nonce_replay_after_reconnect)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'durable-nonce replay scenario committed the first packet on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'durable-nonce replay scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected before durable-nonce replay' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'durable-nonce replay scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=not_committed" 'durable-nonce replay was rejected after reconnect' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=(${seq_one}|${seq_two}) status=" 2 'durable-nonce replay produced exactly two terminal BAM results' "${TIMEOUT_SECS}"
      ;;
    durable_nonce_wrong_authority)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=0 detail=BLOCKHASH_NOT_FOUND" 'wrong-authority durable-nonce batch failed as BLOCKHASH_NOT_FOUND at index 0' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 wrong-authority durable-nonce rejection' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'wrong-authority durable-nonce mode produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    durable_nonce_wrong_authority_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'wrong-authority durable-nonce reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for wrong-authority durable-nonce result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'wrong-authority durable-nonce reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=0 detail=BLOCKHASH_NOT_FOUND" 'wrong-authority durable-nonce reconnect batch failed as BLOCKHASH_NOT_FOUND at index 0' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'wrong-authority durable-nonce reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    bam_fee_priority_replay_after_reconnect)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'priority-fee replay scenario committed the first packet on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'priority-fee replay scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected before priority-fee replay' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'priority-fee replay scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=not_committed .*ALREADY_PROCESSED" 'priority-fee replay was rejected after reconnect' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=(${seq_one}|${seq_two}) status=" 2 'priority-fee replay produced exactly two terminal BAM results' "${TIMEOUT_SECS}"
      ;;
    bam_fee_url_churn_priority_commit|bam_fee_url_churn_same_slot_priority_commit)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first priority-fee BAM transaction committed with the initial fee config' "${TIMEOUT_SECS}"
      require_pattern "${operator_log}" "^operator: set_bam --url ${BAM_BAD_URL_RE}" 'operator runner switched BAM to the alternate fee-config URL' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "Connecting to ${BAM_BAD_URL_RE}" 'validator attempted the alternate BAM URL for fee-config churn' "${TIMEOUT_SECS}"
      require_pattern "${ITER_BAM_SECONDARY_LOG}" '^InitSchedulerStream:' 'validator opened the alternate BAM scheduler stream' "${TIMEOUT_SECS}"
      require_pattern "${ITER_BAM_SECONDARY_LOG}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=1" 'second priority-fee BAM transaction committed with the alternate fee config' "${TIMEOUT_SECS}"
      if [[ "${iter_mode}" == "bam_fee_url_churn_same_slot_priority_commit" ]]; then
        local primary_slot
        local secondary_slot
        primary_slot=$(batch_result_slot_from_log "${bam_log}" "${seq_one}")
        secondary_slot=$(batch_result_slot_from_log "${ITER_BAM_SECONDARY_LOG}" "${seq_two}")
        [[ -n "${primary_slot}" && "${primary_slot}" == "${secondary_slot}" ]]           || die "fullfd BAM fee-config churn did not stay in one slot: primary=${primary_slot:-unknown} secondary=${secondary_slot:-unknown}"
      fi
      ;;
    bam_fee_config_refresh_priority_commit|bam_fee_config_commission_refresh_priority_commit)
      local expected_fee_config_pubkey="${to_one}"
      if [[ "${iter_mode}" == "bam_fee_config_commission_refresh_priority_commit" ]]; then
        expected_fee_config_pubkey="${to_two}"
      fi
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first priority-fee BAM transaction committed before live config refresh' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scripted config_update conn=1 .*prio_fee_recipient_pubkey=${expected_fee_config_pubkey}.*commission_bps=700" 'scenario mutated BamConfig on the live scheduler stream' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "GetBuilderConfig: .*prio_fee_recipient_pubkey=${expected_fee_config_pubkey}.*commission_bps=700" 'validator refreshed GetBuilderConfig after scripted config update' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=1" 'second priority-fee BAM transaction committed after live config refresh' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=committed_batch observed=2' 'scenario observed both config-refresh commits' "${TIMEOUT_SECS}"
      ;;
	    bam_fee_config_refresh_queue_burst|bam_fee_config_refresh_source_mix_queue_burst)
	      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the initial fee-refresh queue BAM bundle' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'initial priority-fee BAM transaction committed before live config refresh' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted config_update conn=1 .*prio_fee_recipient_pubkey=' 'scenario mutated BamConfig before queue pressure' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "GetBuilderConfig: .*prio_fee_recipient_pubkey=${to_one}" 'validator refreshed GetBuilderConfig before queue pressure' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the first post-refresh queue BAM bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'fee-refresh queue scenario closed the scheduler stream' "${TIMEOUT_SECS}"
      if [[ "${ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS}" =~ ^[0-9]+$ && "${ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS}" -gt 0 ]]; then
        require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=batch_result observed=[0-9]+' 'fee-refresh queue scenario observed the initial result plus configured pre-close queue prefix' "${TIMEOUT_SECS}"
	      fi
	      require_pattern_count "${bam_log}" 'scheduler<-validator batch_result seq_id=' "$((1 + ITER_QUEUE_BURST_BATCH_COUNT))" 'validator returned all fee-refresh queue terminal results' "${TIMEOUT_SECS}"
	      if [[ "${iter_mode}" == "bam_fee_config_refresh_source_mix_queue_burst" ]]; then
	        require_pattern "${ITER_DIR}/normal_tpu.log" "sending 1 transactions to 127\\.0\\.0\\.1:${BAM_TPU_PORT}" 'normal TPU packet was sent during fee-refresh queue reconnect' "${TIMEOUT_SECS}"
	      fi
	      ;;
    bam_fee_config_midqueue_refresh|bam_fee_config_midqueue_source_mix_queue_burst|bam_fee_config_midqueue_source_mix_multi_reconnect)
      local midqueue_prefix_count=$((ITER_QUEUE_BURST_BATCH_COUNT / 2))
      local midqueue_observe_count="${ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS}"
      (( midqueue_prefix_count < 1 )) && midqueue_prefix_count=1
      if ! [[ "${midqueue_observe_count}" =~ ^[0-9]+$ ]] || (( midqueue_observe_count < 1 )); then
        midqueue_observe_count=1
      fi
      (( midqueue_observe_count > midqueue_prefix_count )) && midqueue_observe_count="${midqueue_prefix_count}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the initial midqueue fee-refresh BAM bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'initial priority-fee BAM transaction committed before midqueue refresh' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the first old-config queue BAM bundle before midqueue refresh' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=batch_result observed=[0-9]+' 'midqueue scenario observed the initial result plus old-config queue prefix before config refresh' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted config_update conn=1 .*prio_fee_recipient_pubkey=' 'scenario mutated BamConfig while queue work was in flight' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "GetBuilderConfig: .*prio_fee_recipient_pubkey=${to_one}" 'validator refreshed GetBuilderConfig after midqueue config update' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=$((seq_two + midqueue_prefix_count))" 'validator decoded the first post-refresh queue BAM bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'midqueue fee-refresh scenario closed the scheduler stream' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" 'scheduler<-validator batch_result seq_id=' "$((1 + ITER_QUEUE_BURST_BATCH_COUNT))" 'validator returned all midqueue fee-refresh terminal results' "${TIMEOUT_SECS}"
      if [[ "${iter_mode}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${iter_mode}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
        require_pattern "${ITER_DIR}/normal_tpu.log" "sending 1 transactions to 127\\.0\\.0\\.1:${BAM_TPU_PORT}" 'normal TPU packet was sent during midqueue fee-refresh reconnect' "${TIMEOUT_SECS}"
      fi
      if [[ "${iter_mode}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
        require_pattern_count "${bam_log}" 'scripted close_stream' 2 'midqueue multi-reconnect scenario closed the scheduler stream twice' "${TIMEOUT_SECS}"
        require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected to scheduler on conn=2 after midqueue refresh' "${TIMEOUT_SECS}"
        require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=3' 'validator reconnected to scheduler on conn=3 after midqueue refresh' "${TIMEOUT_SECS}"
        require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'midqueue multi-reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
        require_pattern "${bam_log}" 'scripted scenario conn=3 resuming at event' 'midqueue multi-reconnect scenario resumed on conn=3' "${TIMEOUT_SECS}"
        require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=2 kind=batch_result observed=' 'conn=2 observed a durable fee-refresh result before the second close' "${TIMEOUT_SECS}"
      fi
      ;;
    duplicate_seq_split|duplicate_seq_split_reconnect)
      ;;
	    valid_alt_commit)
	      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'valid-ALT v0 transfer committed on conn=1' "${TIMEOUT_SECS}"
	      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=committed_batch observed=1' 'scenario observed valid-ALT transfer committed batch' "${TIMEOUT_SECS}"
	      local observed_to_one
	      observed_to_one=$(wait_for_exact_balance "${to_one}" "${expected_to_one}" "${TIMEOUT_SECS}") || true
	      ITER_OBSERVED_TO_ONE="${observed_to_one}"
	      [[ "${observed_to_one}" == "${expected_to_one}" ]] \
	        || die "valid-ALT recipient ${to_one} ended at ${observed_to_one:-unknown}, expected ${expected_to_one}"
	      ;;
    source_mix_bam_tpu|source_mix_precommit|source_mix_duplicate_tpu_after_bam)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'BAM transaction committed before normal TPU injection' "${TIMEOUT_SECS}"
      require_pattern "${ITER_DIR}/normal_tpu.log" "sending 1 transactions to 127\\.0\\.0\\.1:${BAM_TPU_PORT}" 'normal TPU packet was sent to the validator TPU port' "${TIMEOUT_SECS}"
      if grep -q "scheduler<-validator batch_result seq_id=${seq_two}" "${bam_log}" 2>/dev/null; then
        die "normal TPU packet unexpectedly produced a BAM batch result for seq_id=${seq_two}"
      fi
      ;;
    source_mix_atomic_revert_precommit)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=2 mode=atomic" 'validator decoded the two-packet atomic-revert source-mix bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=1" 'atomic source-mix failure attributed to tx index 1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 atomic source-mix rejection' "${TIMEOUT_SECS}"
      require_pattern "${ITER_DIR}/normal_tpu.log" "sending 1 transactions to 127\\.0\\.0\\.1:${BAM_TPU_PORT}" 'normal TPU packet was sent before the atomic BAM result' "${TIMEOUT_SECS}"
      if grep -q "scheduler<-validator batch_result seq_id=${seq_two}" "${bam_log}" 2>/dev/null; then
        die "normal TPU packet unexpectedly produced a BAM batch result for seq_id=${seq_two}"
      fi
      ;;
    disable_enable_tpu_release)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first BAM transaction committed before disable' "${TIMEOUT_SECS}"
      require_pattern "${operator_log}" '^operator: set_bam --disable' 'operator runner disabled BAM for normal TPU release' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scheduler stream closed by validator conn=1' 'validator closed the scheduler stream after BAM disable' "${TIMEOUT_SECS}"
      require_pattern "${ITER_DIR}/normal_tpu.log" "sending ${NORMAL_TPU_BURST_COUNT} transactions to 127\\.0\\.0\\.1:${BAM_TPU_PORT}" 'normal TPU packet was sent while BAM was disabled' "${TIMEOUT_SECS}"
      require_pattern "${operator_log}" '^operator: set_bam --enable' 'operator runner re-enabled BAM after normal TPU release' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected to scheduler after BAM re-enable' "${TIMEOUT_SECS}"
      if grep -q "scheduler<-validator batch_result seq_id=${seq_two}" "${bam_log}" 2>/dev/null; then
        die "normal TPU release packet unexpectedly produced a BAM batch result for seq_id=${seq_two}"
      fi
      ;;
    queue_burst_multi_reconnect|source_mix_queue_burst_multi_reconnect)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the first multi-reconnect queue BAM bundle' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" 'scripted close_stream' 2 'multi-reconnect scenario closed the scheduler stream twice' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected to scheduler on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=3' 'validator reconnected to scheduler on conn=3' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'multi-reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=3 resuming at event' 'multi-reconnect scenario resumed on conn=3' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=2 kind=batch_result observed=' 'conn=2 observed a durable result before the second close' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" 'scheduler<-validator batch_result seq_id=' "${ITER_QUEUE_BURST_BATCH_COUNT}" 'validator returned all multi-reconnect queue terminal results' "${TIMEOUT_SECS}"
      if [[ "${iter_mode}" == "source_mix_queue_burst_multi_reconnect" ]]; then
        require_pattern "${ITER_DIR}/normal_tpu.log" "sending 1 transactions to 127\\.0\\.0\\.1:${BAM_TPU_PORT}" 'normal TPU packet was sent during multi-reconnect queue pressure' "${TIMEOUT_SECS}"
      fi
      ;;
    partial_drain_reconnect|queue_burst_reconnect|queue_burst64_reconnect|queue_burst64_leader_plus1_reconnect|schedule_boundary_jitter|queue_reconnect_timing_jitter|queue_burst128_reconnect|queue_burst256_reconnect|queue_burst512_reconnect|queue_burst_leader_reconnect|queue_burst64_leader_reconnect|bam_fee_queue_burst_reconnect|bam_fee_source_mix_queue_burst_reconnect|source_mix_queue_burst_reconnect)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the first queue-burst BAM bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'queue-burst scenario closed the scheduler stream' "${TIMEOUT_SECS}"
      if [[ "${ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS}" =~ ^[0-9]+$ && "${ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS}" -gt 0 ]]; then
        require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=batch_result observed=[0-9]+' 'queue-burst scenario observed the configured pre-close result prefix' "${TIMEOUT_SECS}"
      fi
      require_pattern_count "${bam_log}" 'scheduler<-validator batch_result seq_id=' "${ITER_QUEUE_BURST_BATCH_COUNT}" 'validator returned all queue-burst terminal results' "${TIMEOUT_SECS}"
      if [[ "${iter_mode}" == "source_mix_queue_burst_reconnect" || "${iter_mode}" == "bam_fee_source_mix_queue_burst_reconnect" ]]; then
        require_pattern "${ITER_DIR}/normal_tpu.log" "sending 1 transactions to 127\\.0\\.0\\.1:${BAM_TPU_PORT}" 'normal TPU packet was sent during queue reconnect' "${TIMEOUT_SECS}"
      fi
      ;;
    disable_enable_queue_burst_reconnect)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the first disable/enable queue BAM bundle' "${TIMEOUT_SECS}"
      require_pattern "${operator_log}" '^operator: set_bam --disable' 'operator runner disabled BAM during queue pressure' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scheduler stream closed by validator conn=1' 'validator closed scheduler stream after BAM disable' "${TIMEOUT_SECS}"
      require_pattern "${operator_log}" '^operator: set_bam --enable' 'operator runner re-enabled BAM after queue disable' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected to scheduler after BAM re-enable' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" 'scheduler<-validator batch_result seq_id=' "${ITER_QUEUE_BURST_BATCH_COUNT}" 'validator returned all disable/enable queue terminal results' "${TIMEOUT_SECS}"
      ;;
    quarantine_disable_enable_queue_inflight|quarantine_url_churn_queue_inflight)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one}" 'validator decoded the first quarantine queue BAM bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scheduler stream closed by validator conn=1' 'validator closed scheduler stream after quarantine generation change' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected after quarantine generation change' "${TIMEOUT_SECS}"
      require_no_post_generation_batch_results "${bam_log}" "${seq_one}" "${ITER_QUEUE_BURST_BATCH_COUNT}" 'old-generation queue results were not forwarded on the post-change scheduler stream'
      if [[ "${iter_mode}" == "quarantine_disable_enable_queue_inflight" ]]; then
        require_pattern "${operator_log}" '^operator: set_bam --disable' 'operator runner disabled BAM during in-flight queue pressure' "${TIMEOUT_SECS}"
        require_pattern "${operator_log}" '^operator: set_bam --enable' 'operator runner re-enabled BAM after generation churn' "${TIMEOUT_SECS}"
      else
        require_pattern "${operator_log}" "^operator: set_bam --url ${BAM_BAD_URL_RE}" 'operator runner pointed BAM at the bad URL during in-flight queue pressure' "${TIMEOUT_SECS}"
        require_pattern "${operator_log}" "^operator: set_bam --url ${BAM_URL_RE}" 'operator runner restored the BAM URL after generation churn' "${TIMEOUT_SECS}"
        require_pattern "${fd_log}" "Connecting to ${BAM_BAD_URL_RE}" 'validator attempted the temporary bad BAM URL during quarantine' "${TIMEOUT_SECS}"
      fi
      ;;
	    replay_same_conn)
	      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first BAM transaction committed on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the replayed BAM bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=not_committed reason=transaction_error index=0 detail=ALREADY_PROCESSED conn=1" 'same-connection replay rejected as ALREADY_PROCESSED' "${TIMEOUT_SECS}"
      ;;
    replay_after_reconnect|seq_id_max_replay_after_reconnect)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first BAM transaction committed on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event 4 of 8' 'scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the replayed BAM bundle on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=not_committed reason=transaction_error index=0 detail=ALREADY_PROCESSED conn=2" 'reconnect replay rejected as ALREADY_PROCESSED' "${TIMEOUT_SECS}"
      ;;
    unique_after_reconnect|seq_id_wrap_sequence)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first BAM transaction committed on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event 4 of 8' 'scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the second unique BAM bundle on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=2" 'second unique BAM transaction committed on conn=2' "${TIMEOUT_SECS}"
      ;;
    seq_collision_same_conn)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first duplicate-seq payload committed on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=batch_result observed=2' 'scenario observed two terminal same-seq batch results on conn=1' "${TIMEOUT_SECS}"
      ;;
    seq_collision_reconnect)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first duplicate-seq payload committed on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'scenario closed conn=1 before duplicate-seq retry' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event 4 of 8' 'scenario resumed on conn=2 for duplicate-seq retry' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=2 kind=batch_result observed=1' 'scenario observed a terminal same-seq batch result on conn=2' "${TIMEOUT_SECS}"
      ;;
    vote_reject_once)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=0 detail=VOTE_TRANSACTION_FAILURE conn=1" 'simple vote was rejected as VOTE_TRANSACTION_FAILURE on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 not_committed vote batch' "${TIMEOUT_SECS}"
      ;;
    vote_reject_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'vote reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for vote rejection result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'vote reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=0 detail=VOTE_TRANSACTION_FAILURE" 'vote reconnect terminal result returned exactly once across stream churn' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'vote reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    raw_kunorpus_once)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 'raw kunorpus packet produced a terminal BAM result' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=batch_result observed=1' 'scenario observed conn=1 terminal raw-kunorpus batch result' "${TIMEOUT_SECS}"
      ;;
    raw_kunorpus_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'raw kunorpus reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for raw-kunorpus result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'raw kunorpus reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 'raw kunorpus reconnect terminal result returned across stream churn' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'raw kunorpus reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    stale_slot_reject)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=scheduling_error detail=OUTSIDE_LEADER_SLOT conn=1" 'stale max_schedule_slot rejected as OUTSIDE_LEADER_SLOT on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 stale-slot rejection' "${TIMEOUT_SECS}"
      ;;
    stale_slot_reject_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'stale-slot reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for stale-slot result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'stale-slot reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=scheduling_error detail=OUTSIDE_LEADER_SLOT" 'stale-slot reconnect terminal result returned exactly once across stream churn' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'stale-slot reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    empty_batch_reject)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=0 detail=EMPTY conn=1" 'zero-packet batch rejected as EMPTY on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 empty-batch rejection' "${TIMEOUT_SECS}"
      ;;
    empty_batch_reject_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'empty-batch reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for empty-batch result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'empty-batch reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=0 detail=EMPTY" 'empty-batch reconnect terminal result returned exactly once across stream churn' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'empty-batch reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    bam_cu_limit_fail)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=0" 'low-CU BAM transaction failed with transaction_error index 0' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 low-CU rejection' "${TIMEOUT_SECS}"
      ;;
    bam_cu_limit_fail_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'low-CU reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for low-CU result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'low-CU reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=0" 'low-CU reconnect terminal result attributed to tx index 0' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'low-CU reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    malformed_first_atomic)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed" 'malformed-first atomic batch rejected on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 malformed-first rejection' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'malformed-first atomic produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    malformed_first_atomic_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'malformed-first reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for malformed-first result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'malformed-first reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR" 'malformed-first reconnect terminal result attributed to tx index 0' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'malformed-first reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    malformed_tail_atomic)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed" 'malformed-tail atomic batch rejected on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 malformed-tail rejection' "${TIMEOUT_SECS}"
      ;;
    malformed_tail_atomic_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'malformed-tail reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for malformed-tail result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'malformed-tail reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=1 detail=SANITIZE_ERROR" 'malformed-tail reconnect terminal result attributed to tx index 1' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'malformed-tail reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    bad_signature_first_atomic)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed" 'bad-signature-first atomic batch rejected on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 bad-signature-first rejection' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'bad-signature-first atomic produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    bad_signature_first_atomic_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'bad-signature-first reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for bad-signature-first result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'bad-signature-first reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=0 detail=SANITIZE_ERROR" 'bad-signature-first reconnect terminal result attributed to tx index 0' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'bad-signature-first reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    bad_signature_tail_atomic)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed" 'bad-signature-tail atomic batch rejected on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 bad-signature-tail rejection' "${TIMEOUT_SECS}"
      ;;
    bad_signature_tail_atomic_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'bad-signature-tail reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for bad-signature-tail result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'bad-signature-tail reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=1 detail=SANITIZE_ERROR" 'bad-signature-tail reconnect terminal result attributed to tx index 1' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'bad-signature-tail reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    non_atomic_single_packet)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'single-packet non-atomic batch committed on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=committed_batch observed=1' 'scenario observed conn=1 committed non-atomic batch' "${TIMEOUT_SECS}"
      ;;
    atomic_revert)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=2 mode=atomic" 'validator decoded the two-packet atomic-revert bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=1" 'atomic overdraft failure attributed to tx index 1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 atomic revert rejection' "${TIMEOUT_SECS}"
      ;;
    atomic_first_overdraft)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=2 mode=atomic" 'validator decoded the two-packet atomic first-overdraft bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=0" 'atomic first-overdraft failure attributed to tx index 0' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 atomic first-overdraft rejection' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'atomic first-overdraft produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    atomic_first_overdraft_reconnect)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=2 mode=atomic" 'validator decoded the two-packet atomic first-overdraft reconnect bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'atomic first-overdraft reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for atomic first-overdraft result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'atomic first-overdraft reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=0" 'atomic first-overdraft reconnect terminal result attributed to tx index 0' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'atomic first-overdraft reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    atomic_revert_reconnect)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=2 mode=atomic" 'validator decoded the two-packet atomic-revert reconnect bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'atomic-revert reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for atomic-revert result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'atomic-revert reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=1" 'atomic-revert reconnect terminal result attributed to tx index 1' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'atomic-revert reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    atomic_mid_fail)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=3 mode=atomic" 'validator decoded the three-packet atomic-mid-fail bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=1" 'atomic middle execution failure attributed to tx index 1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=not_committed_batch observed=1' 'scenario observed conn=1 atomic middle-failure rejection' "${TIMEOUT_SECS}"
      ;;
    atomic_mid_fail_reconnect)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=3 mode=atomic" 'validator decoded the three-packet atomic-mid-fail reconnect bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted close_stream' 'atomic middle-failure reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for atomic middle-failure result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'atomic middle-failure reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=1" 'atomic middle-failure reconnect terminal result attributed to tx index 1' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'atomic middle-failure reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    atomic_blockhash_mid_fail)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=3 mode=atomic" 'validator decoded the three-packet atomic-blockhash-fail bundle' "${TIMEOUT_SECS}"
      if grep -q "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=1 detail=BLOCKHASH_NOT_FOUND" "${bam_log}" 2>/dev/null; then
        :
      elif grep -q "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=1 detail=SANITIZE_ERROR" "${bam_log}" 2>/dev/null; then
        :
      elif grep -q 'scripted wait_inbound timeout conn=1 kind=batch_result observed=0 expected_at_least=1' "${bam_log}" 2>/dev/null; then
        echo "  warning: atomic_blockhash_mid_fail produced no BAM batch result; preserving incomplete outcome for differential comparison"
      else
        require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=1 detail=BLOCKHASH_NOT_FOUND|reason=deserialization_error index=1 detail=SANITIZE_ERROR|scripted wait_inbound timeout conn=1 kind=batch_result observed=0 expected_at_least=1" 'atomic expired-blockhash middle failure returned index 1 or produced no result' "${TIMEOUT_SECS}"
      fi
      ;;
    atomic_blockhash_mid_fail_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'atomic blockhash reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for atomic blockhash result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'atomic blockhash reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=3 mode=atomic" 'validator decoded the three-packet atomic-blockhash-fail reconnect bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=transaction_error index=1 detail=BLOCKHASH_NOT_FOUND|scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=1 detail=SANITIZE_ERROR" 'atomic expired-blockhash reconnect returned one index-1 rejection' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'atomic blockhash reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    atomic_resolver_mid_fail)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=3 mode=atomic" 'validator decoded the three-packet atomic-resolver-fail bundle' "${TIMEOUT_SECS}"
      if grep -q "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=1" "${bam_log}" 2>/dev/null; then
        :
      elif grep -q 'scripted wait_inbound timeout conn=1 kind=batch_result observed=0 expected_at_least=1' "${bam_log}" 2>/dev/null; then
        echo "  warning: atomic_resolver_mid_fail produced no BAM batch result; preserving incomplete outcome for differential comparison"
      else
        require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=1|scripted wait_inbound timeout conn=1 kind=batch_result observed=0 expected_at_least=1" 'atomic resolver middle failure returned index 1 or produced no result' "${TIMEOUT_SECS}"
      fi
      ;;
    atomic_resolver_mid_fail_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'atomic resolver reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for atomic resolver result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'atomic resolver reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=3 mode=atomic" 'validator decoded the three-packet atomic-resolver-fail reconnect bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed reason=deserialization_error index=1" 'atomic resolver reconnect returned one index-1 rejection' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'atomic resolver reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    atomic_duplicate_sig_mid_fail)
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=3 mode=atomic|scheduler<-validator batch_result seq_id=${seq_one} status=not_committed" 'validator decoded or rejected the atomic duplicate-signature bundle' "${TIMEOUT_SECS}"
      if grep -q "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed" "${bam_log}" 2>/dev/null; then
        require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'atomic duplicate-signature mode produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      elif grep -q 'scripted wait_inbound timeout conn=1 kind=batch_result observed=0 expected_at_least=1' "${bam_log}" 2>/dev/null; then
        echo "  warning: atomic_duplicate_sig_mid_fail produced no BAM batch result; preserving incomplete outcome for differential comparison"
      else
        require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed|scripted wait_inbound timeout conn=1 kind=batch_result observed=0 expected_at_least=1" 'atomic duplicate-signature middle failure returned a rejection or produced no result' "${TIMEOUT_SECS}"
      fi
      ;;
    atomic_duplicate_sig_mid_fail_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'atomic duplicate-signature reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for atomic duplicate-signature result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'atomic duplicate-signature reconnect scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_one} .* txns=3 mode=atomic|scheduler<-validator batch_result seq_id=${seq_one} status=not_committed" 'validator decoded or rejected the atomic duplicate-signature reconnect bundle' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=not_committed" 'atomic duplicate-signature reconnect returned one terminal rejection' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'atomic duplicate-signature reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    seq_id_out_of_order_multi_batch|seq_id_wrap_out_of_order_multi_batch|seq_id_wrap_conflicting_spend_multi_batch|mixed_multi_batch|mixed_empty_multi_batch|mixed_malformed_multi_batch|mixed_bad_signature_multi_batch|mixed_bad_signature_reconnect|mixed_stale_multi_batch|mixed_stale_reconnect|mixed_terminal_producers_reconnect|random_mixed_multi_batch)
      ;;
    external_scenario|generated_bundle)
      ;;
    non_atomic_inconsistent_bundle)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=2 conn=1" 'independent two-transaction batch returned a committed result on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result_tx seq_id=${seq_one} tx_index=0 execution_success=true" 'first independent transaction executed successfully' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result_tx seq_id=${seq_one} tx_index=1 execution_success=false" 'second independent transaction reported execution failure' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=batch_result observed=1' 'scenario observed one terminal independent-batch result' "${TIMEOUT_SECS}"
      ;;
    non_atomic_partial_overdraft_reconnect)
      require_pattern "${bam_log}" 'scripted close_stream' 'non-atomic partial-overdraft reconnect scenario closed conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'InitSchedulerStream:.*conn=2' 'validator reconnected for non-atomic partial-overdraft result drain' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event' 'non-atomic partial-overdraft scenario resumed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 'non-atomic partial-overdraft reconnect batch produced a terminal BAM result' "${TIMEOUT_SECS}"
      require_pattern_count "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 1 'non-atomic partial-overdraft reconnect produced exactly one terminal BAM result' "${TIMEOUT_SECS}"
      ;;
    non_atomic_first_overdraft|non_atomic_mid_overdraft|non_atomic_partial_overdraft|non_atomic_partial_resolver_fail|non_atomic_partial_blockhash_fail|non_atomic_partial_duplicate_sig|non_atomic_partial_cu_fail)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 'non-atomic partial-failure batch produced a terminal BAM result' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=batch_result observed=1' 'scenario observed conn=1 terminal non-atomic partial result' "${TIMEOUT_SECS}"
      ;;
    non_atomic_valid_multi_packet)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=" 'valid non-atomic multi-packet batch produced a terminal BAM result' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted wait_inbound satisfied conn=1 kind=batch_result observed=1' 'scenario observed conn=1 terminal non-atomic result' "${TIMEOUT_SECS}"
      ;;
    invalid_alt_missing_table|invalid_alt_missing_table_reconnect|durable_nonce_wrong_authority|durable_nonce_wrong_authority_reconnect)
      ;;
    disable_enable_unique_after_reconnect)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first BAM transaction committed on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event 3 of 7' 'scenario resumed on conn=2 after disable/enable' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the second unique BAM bundle after disable/enable' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=2" 'second unique BAM transaction committed on conn=2' "${TIMEOUT_SECS}"
      require_pattern "${operator_log}" '^operator: set_bam --disable' 'operator runner disabled BAM' "${TIMEOUT_SECS}"
      require_pattern "${operator_log}" '^operator: set_bam --enable' 'operator runner re-enabled BAM' "${TIMEOUT_SECS}"
      ;;
    url_churn_unique_after_reconnect)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first BAM transaction committed on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=2 resuming at event 3 of 7' 'scenario resumed on conn=2 after URL churn' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "Connecting to ${BAM_BAD_URL_RE}" 'validator attempted the temporary bad BAM URL' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the second unique BAM bundle after URL restore' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=2" 'second unique BAM transaction committed on conn=2 after URL restore' "${TIMEOUT_SECS}"
      require_pattern "${operator_log}" "^operator: set_bam --url ${BAM_BAD_URL_RE}" 'operator runner pointed BAM at the bad URL' "${TIMEOUT_SECS}"
      require_pattern "${operator_log}" "^operator: set_bam --url ${BAM_URL_RE}" 'operator runner restored the BAM URL' "${TIMEOUT_SECS}"
      ;;
    url_sni_churn)
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_one} status=committed txns=1 conn=1" 'first BAM transaction committed on conn=1' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" 'scripted scenario conn=3 resuming at event 3 of 7' 'scenario resumed on conn=3 after URL/SNI churn' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "Connecting to ${BAM_BAD_URL_RE}" 'validator attempted the temporary bad BAM URL during URL/SNI churn' "${TIMEOUT_SECS}"
      require_pattern "${fd_log}" "BAM rx bundle: seq_id=${seq_two}" 'validator decoded the second unique BAM bundle after SNI restore' "${TIMEOUT_SECS}"
      require_pattern "${bam_log}" "scheduler<-validator batch_result seq_id=${seq_two} status=committed txns=1 conn=3" 'second unique BAM transaction committed on conn=3 after SNI restore' "${TIMEOUT_SECS}"
      require_pattern "${operator_log}" "^operator: set_bam --url ${BAM_BAD_URL_RE} --sni churn.invalid" 'operator runner pointed BAM at the bad URL with changed SNI' "${TIMEOUT_SECS}"
      require_pattern "${operator_log}" "^operator: set_bam --url ${BAM_URL_RE} --sni churn.invalid" 'operator runner restored the BAM URL with changed SNI' "${TIMEOUT_SECS}"
      require_pattern "${operator_log}" '^operator: set_bam --sni 127\.0\.0\.1' 'operator runner restored the BAM SNI' "${TIMEOUT_SECS}"
      ;;
    *)
      die "unsupported iteration mode ${iter_mode}"
      ;;
  esac

  echo "  checking balance invariants"
  case "${iter_mode}" in
    external_scenario|generated_bundle)
      echo "  ${iter_mode} has no single-recipient balance invariant"
      ;;
    raw_kunorpus_once|raw_kunorpus_reconnect)
      echo "  raw corpus mode has no balance invariant"
      ;;
	    partial_drain_reconnect|queue_burst_reconnect|queue_burst64_reconnect|queue_burst64_leader_plus1_reconnect|schedule_boundary_jitter|queue_reconnect_timing_jitter|queue_burst_multi_reconnect|source_mix_queue_burst_multi_reconnect|queue_burst128_reconnect|queue_burst256_reconnect|queue_burst512_reconnect|queue_burst_leader_reconnect|queue_burst64_leader_reconnect|source_mix_queue_burst_reconnect|disable_enable_queue_burst_reconnect|quarantine_disable_enable_queue_inflight|quarantine_url_churn_queue_inflight)
      echo "  queue burst has no single-recipient balance invariant"
      ;;
    bam_fee_queue_burst_reconnect|bam_fee_source_mix_queue_burst_reconnect)
      echo "  checking queue burst fee-recipient invariant"
      local observed_queue_recipient_one
      local observed_fee_recipient
      local min_queue_recipient_one=$((initial_to_one + LAMPORTS_ONE))
      local min_fee_recipient=$((initial_to_two + LAMPORTS_TWO))
      observed_queue_recipient_one=$(wait_for_balance_above "${to_one}" "$((min_queue_recipient_one - 1))" "${TIMEOUT_SECS}") || true
      observed_fee_recipient=$(wait_for_balance_above "${to_two}" "$((min_fee_recipient - 1))" "${TIMEOUT_SECS}") || true
      ITER_OBSERVED_TO_ONE="${observed_queue_recipient_one}"
      ITER_OBSERVED_TO_TWO="${observed_fee_recipient}"
      ITER_BAM_FEE_RECIPIENT_OBSERVED="${observed_fee_recipient}"
      [[ "${observed_queue_recipient_one}" =~ ^[0-9]+$ && "${observed_queue_recipient_one}" -ge "${min_queue_recipient_one}" ]] \
        || die "first queue recipient ${to_one} did not reach queue transfer-inclusive balance: got ${observed_queue_recipient_one:-unknown}, minimum ${min_queue_recipient_one}"
      [[ "${observed_fee_recipient}" =~ ^[0-9]+$ && "${observed_fee_recipient}" -ge "${min_fee_recipient}" ]] \
        || die "BAM fee recipient ${to_two} did not reach queue transfer-inclusive balance: got ${observed_fee_recipient:-unknown}, minimum ${min_fee_recipient}"
      ;;
    vote_reject_once|vote_reject_reconnect)
      echo "  checking vote-account invariants"
      if grep -q "BAM rx bundle: seq_id=${seq_one}" "${fd_log}" 2>/dev/null; then
        die "validator unexpectedly decoded the rejected vote bundle"
      fi
      sleep 2
      if [[ "${ITER_VOTE_ACCOUNT_JSON_UNSUPPORTED}" == "1" ]]; then
        ITER_POST_AUTHORIZED_VOTER="jsonParsed_unsupported"
        ITER_POST_VOTES_LEN="jsonParsed_unsupported"
        ITER_POST_LAST_TIMESTAMP_SLOT="jsonParsed_unsupported"
      else
        rpc_vote_account_json "${ITER_VOTE_ACCOUNT}" >"${ITER_DIR}/post_vote.json"
        local post_authorized_voter post_votes_len post_last_timestamp_slot
        post_authorized_voter=$(jq -r '.result.value.data.parsed.info.authorizedVoters[0].authorizedVoter' "${ITER_DIR}/post_vote.json")
        post_votes_len=$(jq -r '.result.value.data.parsed.info.votes | length' "${ITER_DIR}/post_vote.json")
        post_last_timestamp_slot=$(jq -r '.result.value.data.parsed.info.lastTimestamp.slot' "${ITER_DIR}/post_vote.json")
        ITER_POST_AUTHORIZED_VOTER="${post_authorized_voter}"
        ITER_POST_VOTES_LEN="${post_votes_len}"
        ITER_POST_LAST_TIMESTAMP_SLOT="${post_last_timestamp_slot}"
        [[ "${post_authorized_voter}" == "${ITER_PRE_AUTHORIZED_VOTER}" ]] || die "vote account authorized voter changed unexpectedly"
        if [[ "${post_votes_len}" != "${ITER_PRE_VOTES_LEN}" || "${post_last_timestamp_slot}" != "${ITER_PRE_LAST_TIMESTAMP_SLOT}" ]]; then
          echo "  note: vote account history advanced during rejected-vote scenario; fddev normal tower voting is allowed to update this account"
        fi
      fi
      ;;
    non_atomic_valid_multi_packet)
      local observed_to_one
      local observed_to_two
      observed_to_one=$(wait_for_exact_balance "${to_one}" "${expected_to_one}" "${BALANCE_OBSERVE_TIMEOUT_SECS}" 2>/dev/null || true)
      observed_to_two=$(wait_for_exact_balance "${to_two}" "${expected_to_two}" "${BALANCE_OBSERVE_TIMEOUT_SECS}" 2>/dev/null || true)
      ITER_OBSERVED_TO_ONE="${observed_to_one}"
      ITER_OBSERVED_TO_TWO="${observed_to_two}"
      ;;
    non_atomic_inconsistent_bundle|non_atomic_first_overdraft|non_atomic_mid_overdraft|non_atomic_partial_overdraft|non_atomic_partial_overdraft_reconnect|non_atomic_partial_resolver_fail|non_atomic_partial_blockhash_fail|non_atomic_partial_duplicate_sig|non_atomic_partial_cu_fail)
      local observed_to_one
      local observed_to_two
      sleep 3
      observed_to_one=$(rpc_balance "${to_one}" 2>/dev/null || true)
      observed_to_two=$(rpc_balance "${to_two}" 2>/dev/null || true)
      ITER_OBSERVED_TO_ONE="${observed_to_one}"
      ITER_OBSERVED_TO_TWO="${observed_to_two}"
      ;;
    atomic_first_overdraft|atomic_first_overdraft_reconnect|atomic_mid_fail|atomic_mid_fail_reconnect|atomic_blockhash_mid_fail|atomic_blockhash_mid_fail_reconnect|atomic_resolver_mid_fail|atomic_resolver_mid_fail_reconnect|atomic_duplicate_sig_mid_fail|atomic_duplicate_sig_mid_fail_reconnect|stale_slot_reject|stale_slot_reject_reconnect|empty_batch_reject|empty_batch_reject_reconnect|malformed_first_atomic|malformed_first_atomic_reconnect|malformed_tail_atomic|malformed_tail_atomic_reconnect|bad_signature_first_atomic|bad_signature_first_atomic_reconnect|bad_signature_tail_atomic|bad_signature_tail_atomic_reconnect|bam_cu_limit_fail|bam_cu_limit_fail_reconnect)
      local observed_to_one
      local observed_to_two
      observed_to_one=$(rpc_balance "${to_one}" 2>/dev/null || true)
      observed_to_two=$(rpc_balance "${to_two}" 2>/dev/null || true)
      ITER_OBSERVED_TO_ONE="${observed_to_one}"
      ITER_OBSERVED_TO_TWO="${observed_to_two}"
      [[ "${observed_to_one}" == "${initial_to_one}" ]]         || die "first recipient ${to_one} changed unexpectedly: got ${observed_to_one}, expected ${initial_to_one}"
      [[ "${observed_to_two}" == "${initial_to_two}" ]]         || die "second recipient ${to_two} changed unexpectedly: got ${observed_to_two}, expected ${initial_to_two}"
      ;;
    seq_id_wrap_conflicting_spend_multi_batch)
      local observed_to_one
      observed_to_one=$(wait_for_exact_balance "${to_one}" "${expected_to_one}" "${TIMEOUT_SECS}") || true
      ITER_OBSERVED_TO_ONE="${observed_to_one}"
      [[ "${observed_to_one}" == "${expected_to_one}" ]] \
        || die "first recipient ${to_one} ended at ${observed_to_one:-unknown}, expected ${expected_to_one}"

      local observed_to_two
      sleep 2
      observed_to_two=$(rpc_balance "${to_two}" 2>/dev/null || true)
      ITER_OBSERVED_TO_TWO="${observed_to_two}"
      [[ "${observed_to_two}" == "${initial_to_two}" ]] \
        || die "second recipient ${to_two} changed despite expected failed conflicting spend: got ${observed_to_two}, expected ${initial_to_two}"
      ;;
    duplicate_seq_split|duplicate_seq_split_reconnect)
      local observed_to_one
      observed_to_one=$(wait_for_exact_balance "${to_one}" "${expected_to_one}" "${TIMEOUT_SECS}") || true
      ITER_OBSERVED_TO_ONE="${observed_to_one}"
      [[ "${observed_to_one}" == "${expected_to_one}" ]] \
        || die "duplicate-seq recipient ${to_one} ended at ${observed_to_one:-unknown}, expected ${expected_to_one}"
      ;;
    seq_collision_same_conn|seq_collision_reconnect)
      local observed_to_one
      observed_to_one=$(wait_for_exact_balance "${to_one}" "${expected_to_one}" "${TIMEOUT_SECS}") || true
      ITER_OBSERVED_TO_ONE="${observed_to_one}"
      [[ "${observed_to_one}" == "${expected_to_one}" ]] \
        || die "recipient ${to_one} ended at ${observed_to_one:-unknown}, expected ${expected_to_one}"

      local observed_to_two
      sleep 2
      observed_to_two=$(rpc_balance "${to_two}" 2>/dev/null || true)
      ITER_OBSERVED_TO_TWO="${observed_to_two}"
      ITER_EXPECT_TO_TWO="${observed_to_two}"
      ;;
    fee_only_commit|fee_only_reconnect)
      local observed_to_one
      local observed_to_two
      local observed_payer
      observed_to_one=$(wait_for_exact_balance "${to_one}" "${initial_to_one}" "${TIMEOUT_SECS}") || true
      observed_to_two=$(rpc_balance "${to_two}" 2>/dev/null || true)
      observed_payer=$(wait_for_balance_below "${ITER_FROM}" "${ITER_PAYER_INITIAL}" "${TIMEOUT_SECS}") || true
      ITER_OBSERVED_TO_ONE="${observed_to_one}"
      ITER_OBSERVED_TO_TWO="${observed_to_two}"
      ITER_PAYER_OBSERVED="${observed_payer}"
      [[ "${observed_to_one}" == "${initial_to_one}" ]] \
        || die "${iter_mode} recipient ${to_one} changed unexpectedly: got ${observed_to_one:-unknown}, expected ${initial_to_one}"
      [[ "${observed_to_two}" == "${initial_to_two}" ]] \
        || die "${iter_mode} second recipient ${to_two} changed unexpectedly: got ${observed_to_two:-unknown}, expected ${initial_to_two}"
      [[ "${ITER_PAYER_INITIAL}" =~ ^[0-9]+$ && "${observed_payer}" =~ ^[0-9]+$ && "${observed_payer}" -lt "${ITER_PAYER_INITIAL}" ]] \
        || die "${iter_mode} payer ${ITER_FROM} was not debited: got ${observed_payer:-unknown}, initial ${ITER_PAYER_INITIAL:-unknown}"
      ;;
    bam_fee_config_commission_refresh_priority_commit)
      local observed_to_one
      local observed_to_two
      local min_to_two=$((initial_to_two + LAMPORTS_TWO))
      observed_to_one=$(wait_for_exact_balance "${to_one}" "${expected_to_one}" "${TIMEOUT_SECS}") || true
      observed_to_two=$(wait_for_balance_above "${to_two}" "$((min_to_two - 1))" "${TIMEOUT_SECS}") || true
      ITER_OBSERVED_TO_ONE="${observed_to_one}"
      ITER_OBSERVED_TO_TWO="${observed_to_two}"
      ITER_BAM_FEE_RECIPIENT_OBSERVED="${observed_to_two}"
      [[ "${observed_to_one}" == "${expected_to_one}" ]] \
        || die "commission-only fee-refresh recipient ${to_one} ended at ${observed_to_one:-unknown}, expected ${expected_to_one}"
      [[ "${observed_to_two}" =~ ^[0-9]+$ && "${observed_to_two}" -ge "${min_to_two}" ]] \
        || die "commission-only BAM fee recipient ${to_two} did not reach transfer-inclusive balance: got ${observed_to_two:-unknown}, minimum ${min_to_two}"
      ;;
    bam_fee_url_churn_priority_commit|bam_fee_url_churn_same_slot_priority_commit|bam_fee_config_refresh_priority_commit|bam_fee_config_refresh_queue_burst|bam_fee_config_refresh_source_mix_queue_burst|bam_fee_config_midqueue_refresh|bam_fee_config_midqueue_source_mix_queue_burst|bam_fee_config_midqueue_source_mix_multi_reconnect)
      local observed_to_one
      local observed_to_two
      local transfer_one="${ITER_BAM_FEE_CHURN_TRANSFER_ONE:-0}"
      local transfer_two="${ITER_BAM_FEE_CHURN_TRANSFER_TWO:-0}"
      [[ "${transfer_one}" =~ ^[0-9]+$ && "${transfer_two}" =~ ^[0-9]+$ ]] \
        || die "fee-churn transfer lamports were not recorded"
      local min_to_one=$((initial_to_one + transfer_one))
      local min_to_two=$((initial_to_two + transfer_two))
      observed_to_one=$(wait_for_balance_above "${to_one}" "$((min_to_one - 1))" "${TIMEOUT_SECS}") || true
      observed_to_two=$(wait_for_balance_above "${to_two}" "$((min_to_two - 1))" "${TIMEOUT_SECS}") || true
      ITER_OBSERVED_TO_ONE="${observed_to_one}"
      ITER_OBSERVED_TO_TWO="${observed_to_two}"
      ITER_BAM_FEE_RECIPIENT_SECOND_OBSERVED="${observed_to_one}"
      ITER_BAM_FEE_RECIPIENT_OBSERVED="${observed_to_two}"
      [[ "${observed_to_one}" =~ ^[0-9]+$ && "${observed_to_one}" -ge "${min_to_one}" ]] \
        || die "alternate BAM fee recipient ${to_one} did not reach transfer-inclusive balance: got ${observed_to_one:-unknown}, minimum ${min_to_one}"
      [[ "${observed_to_two}" =~ ^[0-9]+$ && "${observed_to_two}" -ge "${min_to_two}" ]] \
        || die "initial BAM fee recipient ${to_two} did not reach transfer-inclusive balance: got ${observed_to_two:-unknown}, minimum ${min_to_two}"
      ;;
    *)
      local observed_to_one
      observed_to_one=$(wait_for_exact_balance "${to_one}" "${expected_to_one}" "${TIMEOUT_SECS}") || true
      ITER_OBSERVED_TO_ONE="${observed_to_one}"
      [[ "${observed_to_one}" == "${expected_to_one}" ]]         || die "recipient ${to_one} ended at ${observed_to_one:-unknown}, expected ${expected_to_one}"

      case "${iter_mode}" in
					        commit_once|replay_same_conn|replay_after_reconnect|source_mix_bam_tpu|source_mix_precommit|source_mix_atomic_revert_precommit|source_mix_duplicate_tpu_after_bam|valid_alt_commit|bam_fee_priority_commit|bam_fee_priority_replay_after_reconnect|durable_nonce_commit|durable_nonce_reconnect|durable_nonce_replay_after_reconnect|atomic_revert_reconnect)
		          if [[ "${iter_mode}" == "source_mix_bam_tpu" || "${iter_mode}" == "source_mix_precommit" || "${iter_mode}" == "source_mix_duplicate_tpu_after_bam" ]]; then
		            sleep 3
		          fi
		          local observed_to_two
		          observed_to_two=""
		          if [[ -n "${to_two}" && "${to_two}" != "n/a" ]]; then
		            observed_to_two=$(rpc_balance "${to_two}" 2>/dev/null || true)
		          fi
		          ITER_OBSERVED_TO_TWO="${observed_to_two}"
			          if [[ "${iter_mode}" == "bam_fee_priority_commit" ]]; then
			            ITER_BAM_FEE_RECIPIENT_OBSERVED="${observed_to_two}"
		          elif [[ "${iter_mode}" == "bam_fee_priority_replay_after_reconnect" ]]; then
		            ITER_BAM_FEE_RECIPIENT_OBSERVED="${observed_to_one}"
		            [[ "${observed_to_two}" == "${initial_to_two}" ]] \
		              || die "second recipient ${to_two} changed unexpectedly: got ${observed_to_two}, expected ${initial_to_two}"
		          elif [[ "${iter_mode}" == "valid_alt_commit" ]]; then
		            :
		          else
	            [[ "${observed_to_two}" == "${initial_to_two}" ]]             || die "second recipient ${to_two} changed unexpectedly: got ${observed_to_two}, expected ${initial_to_two}"
		          fi
	          ;;
        unique_after_reconnect|seq_id_wrap_sequence|seq_id_out_of_order_multi_batch|seq_id_wrap_out_of_order_multi_batch|disable_enable_unique_after_reconnect|disable_enable_tpu_release|url_churn_unique_after_reconnect|url_sni_churn|mixed_multi_batch|mixed_empty_multi_batch|mixed_malformed_multi_batch|mixed_bad_signature_multi_batch|mixed_bad_signature_reconnect|mixed_stale_multi_batch|mixed_stale_reconnect|mixed_terminal_producers_reconnect|random_mixed_multi_batch)
          local observed_to_two
          observed_to_two=$(wait_for_exact_balance "${to_two}" "${expected_to_two}" "${TIMEOUT_SECS}") || true
          ITER_OBSERVED_TO_TWO="${observed_to_two}"
          [[ "${observed_to_two}" == "${expected_to_two}" ]]             || die "second recipient ${to_two} ended at ${observed_to_two:-unknown}, expected ${expected_to_two}"
          ;;
      esac
      ;;
  esac

  if [[ "${iter_mode}" != "stale_slot_reject" && "${iter_mode}" != "stale_slot_reject_reconnect" && "${iter_mode}" != "empty_batch_reject" && "${iter_mode}" != "empty_batch_reject_reconnect" && "${iter_mode}" != "malformed_first_atomic" && "${iter_mode}" != "malformed_first_atomic_reconnect" && "${iter_mode}" != "malformed_tail_atomic" && "${iter_mode}" != "malformed_tail_atomic_reconnect" && "${iter_mode}" != "bad_signature_first_atomic" && "${iter_mode}" != "bad_signature_first_atomic_reconnect" && "${iter_mode}" != "bad_signature_tail_atomic" && "${iter_mode}" != "bad_signature_tail_atomic_reconnect" && "${iter_mode}" != "invalid_alt_missing_table" && "${iter_mode}" != "invalid_alt_missing_table_reconnect" && "${iter_mode}" != "non_atomic_valid_multi_packet" && "${iter_mode}" != "non_atomic_first_overdraft" && "${iter_mode}" != "non_atomic_mid_overdraft" && "${iter_mode}" != "non_atomic_partial_overdraft" && "${iter_mode}" != "non_atomic_partial_overdraft_reconnect" && "${iter_mode}" != "non_atomic_partial_resolver_fail" && "${iter_mode}" != "non_atomic_partial_blockhash_fail" && "${iter_mode}" != "non_atomic_partial_duplicate_sig" && "${iter_mode}" != "non_atomic_partial_cu_fail" && "${iter_mode}" != "bam_cu_limit_fail" && "${iter_mode}" != "bam_cu_limit_fail_reconnect" && "${iter_mode}" != "atomic_first_overdraft_reconnect" && "${iter_mode}" != "atomic_mid_fail_reconnect" && "${iter_mode}" != "atomic_blockhash_mid_fail_reconnect" && "${iter_mode}" != "atomic_resolver_mid_fail_reconnect" && "${iter_mode}" != "atomic_duplicate_sig_mid_fail" && "${iter_mode}" != "atomic_duplicate_sig_mid_fail_reconnect" ]]; then
    case "${ITER_SYSTEM_KIND:-}" in
      create_account|create_account_with_seed)
        echo "  checking create-account owner/space invariants"
        wait_for_account_owner_space "${to_one}" "${ITER_OWNER_ONE}" "${ITER_SPACE_ONE}" "${TIMEOUT_SECS}" \
          || die "created account ${to_one} did not reach owner=${ITER_OWNER_ONE} space=${ITER_SPACE_ONE}"
        ITER_OWNER_ONE_OBSERVED=$(rpc_account_owner "${to_one}" 2>/dev/null || true)
        ITER_SPACE_ONE_OBSERVED=$(rpc_account_space "${to_one}" 2>/dev/null || true)

        case "${iter_mode}" in
          unique_after_reconnect|seq_id_wrap_sequence|seq_id_out_of_order_multi_batch|disable_enable_unique_after_reconnect|url_churn_unique_after_reconnect|url_sni_churn|seq_id_wrap_out_of_order_multi_batch|mixed_multi_batch|mixed_empty_multi_batch|mixed_malformed_multi_batch|mixed_bad_signature_multi_batch|mixed_bad_signature_reconnect|mixed_stale_multi_batch|mixed_stale_reconnect|mixed_terminal_producers_reconnect|random_mixed_multi_batch)
            wait_for_account_owner_space "${to_two}" "${ITER_OWNER_TWO}" "${ITER_SPACE_TWO}" "${TIMEOUT_SECS}" \
              || die "created account ${to_two} did not reach owner=${ITER_OWNER_TWO} space=${ITER_SPACE_TWO}"
            ITER_OWNER_TWO_OBSERVED=$(rpc_account_owner "${to_two}" 2>/dev/null || true)
            ITER_SPACE_TWO_OBSERVED=$(rpc_account_space "${to_two}" 2>/dev/null || true)
            ;;
        esac
        ;;
      assign)
        echo "  checking assign owner/space invariants"
        wait_for_account_owner_space "${to_one}" "${ITER_OWNER_ONE}" "${ITER_SPACE_ONE}" "${TIMEOUT_SECS}" \
          || die "assigned account ${to_one} did not reach owner=${ITER_OWNER_ONE} space=${ITER_SPACE_ONE}"
        ITER_OWNER_ONE_OBSERVED=$(rpc_account_owner "${to_one}" 2>/dev/null || true)
        ITER_SPACE_ONE_OBSERVED=$(rpc_account_space "${to_one}" 2>/dev/null || true)

        case "${iter_mode}" in
          unique_after_reconnect|seq_id_wrap_sequence|seq_id_out_of_order_multi_batch|seq_id_wrap_out_of_order_multi_batch|disable_enable_unique_after_reconnect|url_churn_unique_after_reconnect|url_sni_churn|mixed_multi_batch|mixed_empty_multi_batch|mixed_malformed_multi_batch|mixed_bad_signature_multi_batch|mixed_bad_signature_reconnect|mixed_stale_multi_batch|mixed_stale_reconnect|mixed_terminal_producers_reconnect|random_mixed_multi_batch)
            wait_for_account_owner_space "${to_two}" "${ITER_OWNER_TWO}" "${ITER_SPACE_TWO}" "${TIMEOUT_SECS}" \
              || die "assigned account ${to_two} did not reach owner=${ITER_OWNER_TWO} space=${ITER_SPACE_TWO}"
            ITER_OWNER_TWO_OBSERVED=$(rpc_account_owner "${to_two}" 2>/dev/null || true)
            ITER_SPACE_TWO_OBSERVED=$(rpc_account_space "${to_two}" 2>/dev/null || true)
            ;;
        esac
        ;;
      allocate)
        echo "  checking allocate owner/space invariants"
        wait_for_account_owner_space "${to_one}" "${ITER_OWNER_ONE}" "${ITER_SPACE_ONE}" "${TIMEOUT_SECS}" \
          || die "allocated account ${to_one} did not reach owner=${ITER_OWNER_ONE} space=${ITER_SPACE_ONE}"
        ITER_OWNER_ONE_OBSERVED=$(rpc_account_owner "${to_one}" 2>/dev/null || true)
        ITER_SPACE_ONE_OBSERVED=$(rpc_account_space "${to_one}" 2>/dev/null || true)

        case "${iter_mode}" in
          unique_after_reconnect|seq_id_wrap_sequence|seq_id_out_of_order_multi_batch|seq_id_wrap_out_of_order_multi_batch|disable_enable_unique_after_reconnect|url_churn_unique_after_reconnect|url_sni_churn|mixed_multi_batch|mixed_empty_multi_batch|mixed_malformed_multi_batch|mixed_bad_signature_multi_batch|mixed_bad_signature_reconnect|mixed_stale_multi_batch|mixed_stale_reconnect|mixed_terminal_producers_reconnect|random_mixed_multi_batch)
            wait_for_account_owner_space "${to_two}" "${ITER_OWNER_TWO}" "${ITER_SPACE_TWO}" "${TIMEOUT_SECS}" \
              || die "allocated account ${to_two} did not reach owner=${ITER_OWNER_TWO} space=${ITER_SPACE_TWO}"
            ITER_OWNER_TWO_OBSERVED=$(rpc_account_owner "${to_two}" 2>/dev/null || true)
            ITER_SPACE_TWO_OBSERVED=$(rpc_account_space "${to_two}" 2>/dev/null || true)
            ;;
        esac
        ;;
    esac
  fi

  if [[ -n "${OPERATOR_PID}" ]]; then
    if ! wait "${OPERATOR_PID}"; then
      echo "operator runner failed" >&2
      echo "--- tail ${operator_log} ---" >&2
      tail -n 80 "${operator_log}" >&2 || true
      exit 1
    fi
    OPERATOR_PID=""
  fi
}

echo "logs: ${LOG_DIR}"
echo "config: ${CONFIG}"
echo "rpc: ${RPC_URL}"
echo "bam bind: ${BAM_BIND}"
echo "bam url: ${BAM_URL}"
echo "bam ports: tpu=${BAM_TPU_PORT} tpu_fwd=${BAM_TPU_FWD_PORT} shred=${BAM_SHRED_PORT}"
echo "iterations: ${ITERATIONS}"
echo "base seed: ${SEED}"
echo "target: ${TARGET_NAME}"
echo "runner kind: ${RUNNER_KIND}"
echo "validator binary: ${FDDEV}"
echo "mode: ${MODE}"
echo "input family: ${INPUT_FAMILY}"
echo "kunorpus count: ${KUNORPUS_COUNT}"
echo "kunorpus seed window: ${KUNORPUS_SEED_WINDOW}"
echo "kunorpus max transfer lamports: ${KUNORPUS_MAX_TRANSFER_LAMPORTS}"
echo "kunorpus system kind: ${KUNORPUS_SYSTEM_KIND}"
echo "generated bundle maxima: batches=${GENERATED_BUNDLE_MAX_BATCHES} packets=${GENERATED_BUNDLE_MAX_PACKETS}"
echo "allow operator perturbations: ${ALLOW_OPERATOR_PERTURBATIONS}"
echo "allow self-check failures: ${ALLOW_SELF_CHECK_FAILURES}"
echo "live coverage dir: ${LIVE_COVERAGE_DIR:-disabled}"
echo "check BAM shred delivery: ${CHECK_BAM_SHRED}"
	if [[ -n "${QUEUE_BURST_BATCH_COUNT}" || -n "${QUEUE_BURST_MAX_SCHEDULE_SLOT}" || -n "${QUEUE_BURST_CLOSE_AFTER_RESULTS}" ]]; then
	  echo "queue burst batch count override: ${QUEUE_BURST_BATCH_COUNT:-n/a}"
	  echo "queue burst max_schedule_slot override: ${QUEUE_BURST_MAX_SCHEDULE_SLOT:-n/a}"
	  echo "queue burst close_after_results override: ${QUEUE_BURST_CLOSE_AFTER_RESULTS:-n/a}"
	fi

SELF_CHECK_FAILURES=0
for iter in $(seq 1 "${ITERATIONS}"); do
  cleanup_iteration

  ITER_SEED=$((SEED + iter - 1))
  ITER_MODE=$(choose_mode "${ITER_SEED}" "$((iter - 1))")
  ITER_INPUT_FAMILY=$(choose_input_family "${ITER_SEED}" "${ITER_MODE}")
  ITER_DIR=$(printf '%s/iter_%03d_%s_%s' "${LOG_DIR}" "${iter}" "${ITER_MODE}" "${ITER_INPUT_FAMILY}")
  mkdir -p "${ITER_DIR}"
  write_repro "${ITER_DIR}" "${ITER_SEED}" "${ITER_MODE}" "${ITER_INPUT_FAMILY}"

  FD_LOG="${ITER_DIR}/fd.log"
  BAM_LOG="${ITER_DIR}/bam.log"
  OPERATOR_LOG="${ITER_DIR}/operator.log"
  INPUT_PATH="${ITER_DIR}/input.txnctx"
  PACKET_ONE_PATH="${ITER_DIR}/packet_one.txt"
  PACKET_TWO_PATH="${ITER_DIR}/packet_two.txt"
	  PACKET_BATCH_PATH="${ITER_DIR}/packet_batch.txt"
	  NORMAL_TPU_LOG="${ITER_DIR}/normal_tpu.log"
	  NORMAL_TPU_PACKET_PATH="${ITER_DIR}/normal_tpu_packet.txt"
	  NORMAL_TPU_BRIDGE_OUT="${ITER_DIR}/normal_tpu_bridge.out"
	  PROBE_OUT="${ITER_DIR}/probe.out"
  PROBE_ERR="${ITER_DIR}/probe.err"
  BAM_SHRED_CAPTURE_PATH="${ITER_DIR}/bam_shred_capture.json"
  BRIDGE_ONE_OUT="${ITER_DIR}/bridge_one.out"
  BRIDGE_TWO_OUT="${ITER_DIR}/bridge_two.out"
  GEN_OUT="${ITER_DIR}/gen.out"
  SCENARIO_PATH="${ITER_DIR}/scenario.toml"
  EVENTS_PATH="${ITER_DIR}/events.txt"

  SEQ_BASE=$((1000 + ITER_SEED * 10))
  SEQ_ONE=${SEQ_BASE}
  SEQ_TWO=$((SEQ_BASE + 1))
  SEQ_THREE=$((SEQ_BASE + 2))
  if [[ "${ITER_MODE}" == "seq_id_max_once" || "${ITER_MODE}" == "seq_id_max_replay_after_reconnect" ]]; then
    SEQ_ONE=4294967295
    SEQ_TWO=1
  elif [[ "${ITER_MODE}" == "seq_id_wrap_sequence" ]]; then
    SEQ_ONE=4294967294
    SEQ_TWO=1
  elif [[ "${ITER_MODE}" == "seq_id_wrap_out_of_order_multi_batch" || "${ITER_MODE}" == "seq_id_wrap_conflicting_spend_multi_batch" ]]; then
    SEQ_ONE=0
    SEQ_TWO=4294967295
  fi
  ITER_RANDOM_MIX_PATTERN=""
  CURRENT_BAM_BUILDER_PUBKEY=""
  CURRENT_BAM_OMIT_BLOCK_ENGINE_CONFIG=0
  VALID_ALT_CREATE_PACKET=""
  VALID_ALT_EXTEND_PACKET=""
  VALID_ALT_LOOKUP_TABLE=""
  ITER_BAM_FEE_RECIPIENT=""
  ITER_BAM_FEE_RECIPIENT_INITIAL=""
  ITER_BAM_FEE_RECIPIENT_OBSERVED=""
  ITER_BAM_FEE_RECIPIENT_SECOND=""
  ITER_BAM_FEE_RECIPIENT_SECOND_INITIAL=""
  ITER_BAM_FEE_RECIPIENT_SECOND_OBSERVED=""
  ITER_BAM_FEE_CHURN_TRANSFER_ONE=""
  ITER_BAM_FEE_CHURN_TRANSFER_TWO=""
  ITER_BAM_SECONDARY_LOG=""
  ITER_NORMAL_TPU_EXPECTED_LANDED=""
  ITER_BAM_SHRED_CAPTURE=""
  ITER_BAM_SHRED_CAPTURE_PACKET_COUNT=""
  ITER_BAM_SHRED_CAPTURE_BYTE_COUNT=""
  if [[ "${ITER_MODE}" == "mixed_terminal_producers_reconnect" ]]; then
    SEQ_TWO=$((SEQ_BASE + 4))
  elif [[ "${ITER_MODE}" == "mixed_multi_batch" || "${ITER_MODE}" == "mixed_empty_multi_batch" || "${ITER_MODE}" == "mixed_malformed_multi_batch" || "${ITER_MODE}" == "mixed_bad_signature_multi_batch" || "${ITER_MODE}" == "mixed_bad_signature_reconnect" || "${ITER_MODE}" == "mixed_stale_multi_batch" || "${ITER_MODE}" == "mixed_stale_reconnect" ]]; then
    SEQ_TWO=$((SEQ_BASE + 2))
  elif [[ "${ITER_MODE}" == "random_mixed_multi_batch" ]]; then
    ITER_RANDOM_MIX_PATTERN=$(random_mixed_pattern_for_seed "${ITER_SEED}")
    SEQ_TWO=$((SEQ_BASE + $(random_mixed_valid2_offset "${ITER_RANDOM_MIX_PATTERN}")))
  fi
  LAMPORTS=$((10000 + (ITER_SEED * 7919) % 50000))
  CU_LIMIT=$((200000 + (ITER_SEED * 3571) % 100000))
  ITER_QUEUE_BURST_BATCH_COUNT=""
  ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT=""
  ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS=""
  ITER_QUEUE_BURST_SECOND_CLOSE_AFTER_RESULTS=""
  ITER_QUEUE_PACKET_FILES=()
  if is_queue_burst_mode "${ITER_MODE}"; then
    configure_queue_burst_params "${ITER_MODE}" "${ITER_SEED}"
    SEQ_ONE=200
    SEQ_TWO=201
  fi

  echo
  echo "== iteration ${iter}/${ITERATIONS}: seed=${ITER_SEED} mode=${ITER_MODE} input_family=${ITER_INPUT_FAMILY} lamports=${LAMPORTS} cu_limit=${CU_LIMIT} =="

  helper_rc=0
  if run_helper_mode "${ITER_MODE}" "${ITER_DIR}" "${ITER_SEED}" "${ITER_INPUT_FAMILY}" "${LAMPORTS}"; then
    echo "  iteration passed via helper-backed mode; artifacts: ${ITER_DIR}"
    continue
  else
    helper_rc=$?
  fi
  if [[ "${helper_rc}" -ne 2 ]]; then
    exit "${helper_rc}"
  fi

  FULLFD_BOOTSTRAP_BAM=0
  if [[ "${RUNNER_KIND}" == "fullfd" && "${ITER_MODE}" == "external_scenario" ]]; then
    BAM_BOOTSTRAP_SCENARIO="${ITER_DIR}/bam-bootstrap.toml"
    BAM_BOOTSTRAP_LOG="${ITER_DIR}/bam-bootstrap.log"
    write_fullfd_bootstrap_scenario "${BAM_BOOTSTRAP_SCENARIO}"
    start_bam_controller "${ITER_DIR}" "${BAM_BOOTSTRAP_LOG}" "${BAM_BOOTSTRAP_SCENARIO}"
    FULLFD_BOOTSTRAP_BAM=1
  fi

  if [[ "${CHECK_BAM_SHRED}" == "1" ]]; then
    start_bam_shred_capture "${ITER_DIR}" "${BAM_SHRED_CAPTURE_PATH}"
  fi

  start_fddev "${ITER_DIR}" "${FD_LOG}"

  ITER_RPC_READY_TIMEOUT_SECS="${RPC_READY_TIMEOUT_SECS}"
  if [[ "${ITER_MODE}" == "bam_fee_url_churn_same_slot_priority_commit" ]]; then
    ITER_RPC_READY_TIMEOUT_SECS="${TIMEOUT_SECS}"
  fi
  wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}" >/dev/null || die "validator RPC did not become ready at ${RPC_URL}"

  PREPARED_INPUT_PATH=""
  PREPARED_INPUT_LABEL=""
  PREPARED_INPUT_NOTE=""
  ITER_SYSTEM_KIND=""
  ITER_OWNER_ONE=""
  ITER_OWNER_TWO=""
  ITER_SPACE_ONE=""
  ITER_SPACE_TWO=""
  ITER_OWNER_ONE_OBSERVED=""
  ITER_OWNER_TWO_OBSERVED=""
  ITER_SPACE_ONE_OBSERVED=""
  ITER_SPACE_TWO_OBSERVED=""
  ITER_SIMPLE_VOTE_NODE=""
  ITER_VOTE_ACCOUNT=""
  ITER_AUTHORIZED_VOTER=""
  ITER_VOTE_SLOT=""
  ITER_VOTE_HASH=""
  ITER_PRE_AUTHORIZED_VOTER=""
  ITER_PRE_VOTES_LEN=""
  ITER_PRE_LAST_TIMESTAMP_SLOT=""
  ITER_VOTE_ACCOUNT_JSON_UNSUPPORTED=0
  ITER_OBSERVED_TO_ONE=""
  ITER_OBSERVED_TO_TWO=""
  ITER_PAYER_INITIAL=""
  ITER_PAYER_OBSERVED=""
  ITER_DURABLE_NONCE_ACCOUNT=""
  ITER_DURABLE_NONCE_HASH=""
  ITER_POST_AUTHORIZED_VOTER=""
  ITER_POST_VOTES_LEN=""
  ITER_POST_LAST_TIMESTAMP_SLOT=""
  ITER_FROM="n/a"
  ITER_TO_ONE="n/a"
  ITER_TO_TWO="n/a"
  ITER_INITIAL_TO_ONE=""
  ITER_INITIAL_TO_TWO=""
  ITER_EXPECT_TO_ONE=""
  ITER_EXPECT_TO_TWO=""
  EXPECTED_TERMINAL_RESULTS=""
  BLOCKHASH="n/a"
  if [[ "${ITER_MODE}" == "generated_bundle" ]]; then
    GENERATED_RECIPE_PATH="${ITER_DIR}/scenario.recipe.toml"
    GENERATED_MATERIALIZE_META="${ITER_DIR}/generated-materialized.env"
    GENERATED_PACKET_DIR="${ITER_DIR}/generated-packets"
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    materialize_generated_bundle_scenario \
      "${ITER_SEED}" \
      "${GENERATED_RECIPE_PATH}" \
      "${SCENARIO_PATH}" \
      "${GENERATED_PACKET_DIR}" \
      "${GENERATED_MATERIALIZE_META}" \
      "${BLOCKHASH}"
    EXPECTED_TERMINAL_RESULTS=$(metadata_get "${GENERATED_MATERIALIZE_META}" expected_terminal_results)
    MATERIALIZED_PACKET_COUNT=$(metadata_get "${GENERATED_MATERIALIZE_META}" materialized_packet_count)
    GENERATED_FAMILY_COUNTS=$(metadata_get "${GENERATED_MATERIALIZE_META}" generated_family_counts)
    ITER_SYSTEM_KIND="heterogeneous"
    ITER_FROM=$(metadata_get "${GENERATED_MATERIALIZE_META}" payer)
    ITER_TO_ONE=$(metadata_get "${GENERATED_MATERIALIZE_META}" first_recipient)
    ITER_TO_TWO=$(metadata_get "${GENERATED_MATERIALIZE_META}" last_recipient)
    [[ -n "${ITER_FROM}" ]] || ITER_FROM="n/a"
    [[ -n "${ITER_TO_ONE}" ]] || ITER_TO_ONE="n/a"
    [[ -n "${ITER_TO_TWO}" ]] || ITER_TO_TWO="n/a"
    if [[ "${ITER_FROM}" != "n/a" ]]; then
      ITER_PAYER_INITIAL=$(rpc_balance "${ITER_FROM}" 2>/dev/null || true)
    fi
    if [[ "${ITER_TO_ONE}" != "n/a" ]]; then
      ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}" 2>/dev/null || true)
    fi
    if [[ "${ITER_TO_TWO}" != "n/a" ]]; then
      ITER_INITIAL_TO_TWO=$(rpc_balance "${ITER_TO_TWO}" 2>/dev/null || true)
    fi
    PREPARED_INPUT_PATH="${GENERATED_RECIPE_PATH}"
    PREPARED_INPUT_LABEL="generated_heterogeneous_bundle"
    PREPARED_INPUT_NOTE="generated_bundle_seed=${ITER_SEED} max_batches=${GENERATED_BUNDLE_MAX_BATCHES} max_packets=${GENERATED_BUNDLE_MAX_PACKETS} materialized_packet_count=${MATERIALIZED_PACKET_COUNT} expected_terminal_results=${EXPECTED_TERMINAL_RESULTS} family_counts=${GENERATED_FAMILY_COUNTS}"
  elif [[ "${ITER_MODE}" == "external_scenario" ]]; then
    EXTERNAL_SOURCE_SCENARIO_PATH="${ITER_DIR}/scenario.external.toml"
    EXTERNAL_MATERIALIZE_META="${ITER_DIR}/external-materialized.env"
    EXTERNAL_PACKET_DIR="${ITER_DIR}/external-packets"
    cp "${EXTERNAL_SCENARIO_FILE}" "${EXTERNAL_SOURCE_SCENARIO_PATH}"
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    materialize_external_scenario \
      "${EXTERNAL_SOURCE_SCENARIO_PATH}" \
      "${SCENARIO_PATH}" \
      "${EXTERNAL_PACKET_DIR}" \
      "${EXTERNAL_MATERIALIZE_META}" \
      "${BLOCKHASH}"
    if [[ "${FULLFD_BOOTSTRAP_BAM}" == "1" ]]; then
      stop_bam_controller
      FULLFD_BOOTSTRAP_BAM=0
    fi
    MATERIALIZED_PACKET_COUNT=$(metadata_get "${EXTERNAL_MATERIALIZE_META}" materialized_packet_count)
    MATERIALIZED_PACKET_SETS=$(metadata_get "${EXTERNAL_MATERIALIZE_META}" materialized_packet_sets)
    EXPECTED_TERMINAL_RESULTS=$(metadata_get "${EXTERNAL_MATERIALIZE_META}" expected_terminal_results)
    ITER_FROM=$(metadata_get "${EXTERNAL_MATERIALIZE_META}" payer)
    ITER_TO_ONE=$(metadata_get "${EXTERNAL_MATERIALIZE_META}" first_recipient)
    ITER_TO_TWO=$(metadata_get "${EXTERNAL_MATERIALIZE_META}" last_recipient)
    [[ -n "${ITER_FROM}" ]] || ITER_FROM="n/a"
    [[ -n "${ITER_TO_ONE}" ]] || ITER_TO_ONE="n/a"
    [[ -n "${ITER_TO_TWO}" ]] || ITER_TO_TWO="n/a"
    if [[ "${ITER_FROM}" != "n/a" ]]; then
      ITER_PAYER_INITIAL=$(rpc_balance "${ITER_FROM}" 2>/dev/null || true)
    fi
    if [[ "${ITER_TO_ONE}" != "n/a" ]]; then
      ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}" 2>/dev/null || true)
    fi
    if [[ "${ITER_TO_TWO}" != "n/a" ]]; then
      ITER_INITIAL_TO_TWO=$(rpc_balance "${ITER_TO_TWO}" 2>/dev/null || true)
    fi
    if [[ "${MATERIALIZED_PACKET_COUNT:-0}" =~ ^[0-9]+$ && "${MATERIALIZED_PACKET_COUNT:-0}" -gt 0 ]]; then
      ITER_SYSTEM_KIND="transfer"
    else
      ITER_SYSTEM_KIND=""
    fi
    PREPARED_INPUT_PATH="${EXTERNAL_SOURCE_SCENARIO_PATH}"
    PREPARED_INPUT_LABEL="external_scripted_scenario"
    PREPARED_INPUT_NOTE="external_scenario_file=${EXTERNAL_SOURCE_SCENARIO_PATH} materialized_packet_count=${MATERIALIZED_PACKET_COUNT:-0} materialized_packet_sets=${MATERIALIZED_PACKET_SETS:-0} materialized_blockhash=${BLOCKHASH}"
  elif [[ "${ITER_MODE}" == "non_atomic_inconsistent_bundle" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_non_atomic_inconsistent_bundle_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"
  elif [[ "${ITER_MODE}" == "non_atomic_first_overdraft" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_non_atomic_first_overdraft_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"
  elif [[ "${ITER_MODE}" == "non_atomic_partial_overdraft" || "${ITER_MODE}" == "non_atomic_partial_overdraft_reconnect" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_non_atomic_partial_overdraft_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"
  elif [[ "${ITER_MODE}" == "non_atomic_mid_overdraft" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_non_atomic_mid_overdraft_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"
  elif [[ "${ITER_MODE}" == "non_atomic_partial_resolver_fail" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_non_atomic_partial_resolver_fail_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"
  elif [[ "${ITER_MODE}" == "non_atomic_partial_blockhash_fail" ]]; then
    OLD_BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide an old blockhash at ${RPC_URL}"
    OLD_SLOT=$(rpc_slot 2>/dev/null || printf '0')
    WAITED_SLOT=$(wait_rpc_slot_at_least "$((OLD_SLOT + 170))" "${TIMEOUT_SECS}") \
      || die "validator slot did not advance far enough to expire old blockhash; old_slot=${OLD_SLOT} latest=${WAITED_SLOT}"
    FRESH_BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh post-expiry blockhash at ${RPC_URL}"
    BLOCKHASH="${OLD_BLOCKHASH}->${FRESH_BLOCKHASH}"
    prepare_non_atomic_partial_blockhash_fail_packets       "${ITER_DIR}"       "${OLD_BLOCKHASH}"       "${FRESH_BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"       "${OLD_SLOT}"       "${WAITED_SLOT}"
  elif [[ "${ITER_MODE}" == "non_atomic_partial_duplicate_sig" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_non_atomic_partial_duplicate_sig_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"
  elif [[ "${ITER_MODE}" == "non_atomic_partial_cu_fail" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_non_atomic_partial_cu_fail_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"
  elif [[ "${ITER_MODE}" == "duplicate_seq_split" || "${ITER_MODE}" == "duplicate_seq_split_reconnect" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_duplicate_seq_split_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_BATCH_PATH}"       "${LAMPORTS}"
  elif [[ "${ITER_MODE}" == "seq_id_wrap_conflicting_spend_multi_batch" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_seq_id_wrap_conflicting_spend_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"
  elif [[ "${ITER_MODE}" == "atomic_revert" || "${ITER_MODE}" == "atomic_revert_reconnect" || "${ITER_MODE}" == "source_mix_atomic_revert_precommit" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_atomic_revert_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"
  elif [[ "${ITER_MODE}" == "atomic_first_overdraft" || "${ITER_MODE}" == "atomic_first_overdraft_reconnect" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_atomic_first_overdraft_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"
  elif [[ "${ITER_MODE}" == "atomic_mid_fail" || "${ITER_MODE}" == "atomic_mid_fail_reconnect" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_atomic_mid_fail_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"
  elif [[ "${ITER_MODE}" == "atomic_blockhash_mid_fail" || "${ITER_MODE}" == "atomic_blockhash_mid_fail_reconnect" ]]; then
    OLD_BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide an old blockhash at ${RPC_URL}"
    OLD_SLOT=$(rpc_slot 2>/dev/null || printf '0')
    WAITED_SLOT=$(wait_rpc_slot_at_least "$((OLD_SLOT + 170))" "${TIMEOUT_SECS}") \
      || die "validator slot did not advance far enough to expire old blockhash; old_slot=${OLD_SLOT} latest=${WAITED_SLOT}"
    FRESH_BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh post-expiry blockhash at ${RPC_URL}"
    BLOCKHASH="${OLD_BLOCKHASH}->${FRESH_BLOCKHASH}"
    prepare_atomic_blockhash_mid_fail_packets       "${ITER_DIR}"       "${OLD_BLOCKHASH}"       "${FRESH_BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"       "${OLD_SLOT}"       "${WAITED_SLOT}"
  elif [[ "${ITER_MODE}" == "atomic_resolver_mid_fail" || "${ITER_MODE}" == "atomic_resolver_mid_fail_reconnect" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_atomic_resolver_mid_fail_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"
  elif [[ "${ITER_MODE}" == "atomic_duplicate_sig_mid_fail" || "${ITER_MODE}" == "atomic_duplicate_sig_mid_fail_reconnect" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_atomic_duplicate_sig_mid_fail_packets       "${ITER_DIR}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${PACKET_TWO_PATH}"       "${PACKET_BATCH_PATH}"       "${BRIDGE_ONE_OUT}"       "${BRIDGE_TWO_OUT}"
  elif [[ "${ITER_MODE}" == "valid_alt_commit" ]]; then
    WAITED_FINALIZED_SLOT=$(wait_rpc_finalized_slot_at_least "${BAM_VALID_ALT_MIN_FINALIZED_SLOT}" "${TIMEOUT_SECS}") \
      || die "validator finalized slot did not reach ${BAM_VALID_ALT_MIN_FINALIZED_SLOT} for valid ALT test; latest finalized slot=${WAITED_FINALIZED_SLOT}"
    echo "  valid ALT finalized slot ready: ${WAITED_FINALIZED_SLOT}"
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_valid_alt_commit_packet "${ITER_DIR}" "${BLOCKHASH}" "${PACKET_ONE_PATH}" "${PACKET_TWO_PATH}" "${PACKET_BATCH_PATH}" "${BRIDGE_ONE_OUT}" "${LAMPORTS}"
  elif [[ "${ITER_MODE}" == "invalid_alt_missing_table" || "${ITER_MODE}" == "invalid_alt_missing_table_reconnect" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_invalid_alt_missing_table_packet "${ITER_DIR}" "${BLOCKHASH}" "${PACKET_ONE_PATH}" "${BRIDGE_ONE_OUT}" "${LAMPORTS}"
  elif [[ "${ITER_MODE}" == "durable_nonce_commit" || "${ITER_MODE}" == "durable_nonce_reconnect" || "${ITER_MODE}" == "durable_nonce_replay_after_reconnect" || "${ITER_MODE}" == "durable_nonce_wrong_authority" || "${ITER_MODE}" == "durable_nonce_wrong_authority_reconnect" ]]; then
    BLOCKHASH="durable-nonce"
    WRONG_NONCE_AUTHORITY=0
    if [[ "${ITER_MODE}" == "durable_nonce_wrong_authority" || "${ITER_MODE}" == "durable_nonce_wrong_authority_reconnect" ]]; then
      WRONG_NONCE_AUTHORITY=1
    fi
    prepare_durable_nonce_commit_packet "${ITER_DIR}" "${PACKET_ONE_PATH}" "${GEN_OUT}" "${LAMPORTS}" "${ITER_SEED}" "${WRONG_NONCE_AUTHORITY}"
  elif [[ "${ITER_MODE}" == "raw_kunorpus_once" || "${ITER_MODE}" == "raw_kunorpus_reconnect" ]]; then
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    prepare_kunorpus_raw_txn_input       "${ITER_DIR}"       "${ITER_SEED}"       "${PACKET_ONE_PATH}"       "${BRIDGE_ONE_OUT}"       "${PROBE_ERR}"
	  elif [[ "${ITER_MODE}" == "vote_reject_once" || "${ITER_MODE}" == "vote_reject_reconnect" ]]; then
	    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
	    prepare_kunorpus_vote_input       "${ITER_DIR}"       "${ITER_SEED}"       "${BLOCKHASH}"       "${PACKET_ONE_PATH}"       "${BRIDGE_ONE_OUT}"       "${PROBE_ERR}"
	  else
	    if [[ "${ITER_MODE}" == "source_mix_bam_tpu" || "${ITER_MODE}" == "source_mix_precommit" || "${ITER_MODE}" == "source_mix_duplicate_tpu_after_bam" || "${ITER_MODE}" == "disable_enable_tpu_release" ]]; then
	      "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
	        --output "${INPUT_PATH}" \
	        --system-kind transfer \
	        --transfer-count 1 \
	        --lamports "${LAMPORTS}" \
	        --cu-limit "${CU_LIMIT}" \
	        >"${GEN_OUT}"
	      PREPARED_INPUT_PATH="${INPUT_PATH}"
	      PREPARED_INPUT_LABEL="synthetic_transfer_txnctx"
	      PREPARED_INPUT_NOTE="system_kind=transfer lamports=${LAMPORTS} transfer_count=1 source_mix_mode=${ITER_MODE} normal_tpu_port=${BAM_TPU_PORT}"
	      if [[ "${ITER_MODE}" == "disable_enable_tpu_release" ]]; then
	        PREPARED_INPUT_NOTE="${PREPARED_INPUT_NOTE} normal_tpu_expected_landed=true"
	      fi
	    elif [[ "${ITER_MODE}" == "bam_fee_priority_commit" || "${ITER_MODE}" == "bam_fee_priority_replay_after_reconnect" || ( "${ITER_MODE}" == "bam_fee_url_churn_priority_commit" || "${ITER_MODE}" == "bam_fee_url_churn_same_slot_priority_commit" || "${ITER_MODE}" == "bam_fee_config_refresh_priority_commit" || "${ITER_MODE}" == "bam_fee_config_commission_refresh_priority_commit" ) ]]; then
	      "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
	        --output "${INPUT_PATH}" \
	        --system-kind transfer \
	        --transfer-count 1 \
	        --lamports "${LAMPORTS}" \
	        --cu-limit 300000 \
	        --cu-price 50000 \
	        >"${GEN_OUT}"
	      PREPARED_INPUT_PATH="${INPUT_PATH}"
	      PREPARED_INPUT_LABEL="synthetic_priority_fee_transfer_txnctx"
	      PREPARED_INPUT_NOTE="system_kind=transfer lamports=${LAMPORTS} transfer_count=1 cu_limit=300000 cu_price=50000 bam_fee_priority=true fee_config_mode=${ITER_MODE}"
	    elif [[ "${ITER_MODE}" == "fee_only_commit" || "${ITER_MODE}" == "fee_only_reconnect" ]]; then
	      "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
	        --output "${INPUT_PATH}" \
	        --system-kind fee-only \
	        --transfer-count 1 \
	        --lamports 0 \
	        --cu-limit 300000 \
	        --cu-price 50000 \
	        >"${GEN_OUT}"
	      PREPARED_INPUT_PATH="${INPUT_PATH}"
	      PREPARED_INPUT_LABEL="synthetic_fee_only_txnctx"
	      PREPARED_INPUT_NOTE="system_kind=fee_only lamports=0 transfer_count=0 cu_limit=300000 cu_price=50000 fee_only_mode=${ITER_MODE}"
    elif [[ "${ITER_MODE}" == "bam_cu_limit_fail" || "${ITER_MODE}" == "bam_cu_limit_fail_reconnect" ]]; then
	      "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
	        --output "${INPUT_PATH}" \
	        --system-kind transfer \
	        --transfer-count 1 \
	        --lamports "${LAMPORTS}" \
	        --cu-limit 1 \
	        >"${GEN_OUT}"
	      PREPARED_INPUT_PATH="${INPUT_PATH}"
	      PREPARED_INPUT_LABEL="synthetic_low_cu_transfer_txnctx"
      PREPARED_INPUT_NOTE="system_kind=transfer lamports=${LAMPORTS} transfer_count=1 cu_limit=1 bam_cu_limit_fail=true"
    elif is_queue_burst_mode "${ITER_MODE}"; then
      BURST_LAMPORTS="${LAMPORTS}"
      if [[ "${BURST_LAMPORTS}" =~ ^[0-9]+$ ]] && (( BURST_LAMPORTS < 1000000 )); then
        BURST_LAMPORTS=1000000
      fi
      BURST_TRANSFER_COUNT=1
      BURST_CU_LIMIT=300000
      BURST_EXTRA_ARGS=()
      if [[ "${ITER_MODE}" == "quarantine_disable_enable_queue_inflight" || "${ITER_MODE}" == "quarantine_url_churn_queue_inflight" ]]; then
        BURST_TRANSFER_COUNT=8
        BURST_CU_LIMIT=1000000
        BURST_EXTRA_ARGS+=( --lamports-step 1 )
      fi
      if [[ "${ITER_MODE}" == "bam_fee_queue_burst_reconnect" || "${ITER_MODE}" == "bam_fee_source_mix_queue_burst_reconnect" || "${ITER_MODE}" == "bam_fee_config_refresh_queue_burst" || "${ITER_MODE}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_refresh" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
        BURST_EXTRA_ARGS+=( --cu-price 50000 )
      fi
      "${GEN_SIMPLE_SYSTEM_TXNCTX_BIN}" \
        --output "${INPUT_PATH}" \
        --system-kind transfer \
        --transfer-count "${BURST_TRANSFER_COUNT}" \
        --lamports "${BURST_LAMPORTS}" \
        --cu-limit "${BURST_CU_LIMIT}" \
        "${BURST_EXTRA_ARGS[@]}" \
        >"${GEN_OUT}"
      PREPARED_INPUT_PATH="${INPUT_PATH}"
      if [[ "${ITER_MODE}" == "bam_fee_queue_burst_reconnect" || "${ITER_MODE}" == "bam_fee_source_mix_queue_burst_reconnect" || "${ITER_MODE}" == "bam_fee_config_refresh_queue_burst" || "${ITER_MODE}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_refresh" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
        PREPARED_INPUT_LABEL="synthetic_fee_queue_burst_transfer_txnctx"
        PREPARED_INPUT_NOTE="system_kind=transfer lamports=${BURST_LAMPORTS} transfer_count=1 queue_burst=true batch_count=${ITER_QUEUE_BURST_BATCH_COUNT} max_schedule_slot=${ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT} close_after_results=${ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS} cu_limit=300000 cu_price=50000 bam_fee_queue_burst=true"
        if [[ "${ITER_MODE}" == "bam_fee_config_refresh_queue_burst" || "${ITER_MODE}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_refresh" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
          PREPARED_INPUT_NOTE="${PREPARED_INPUT_NOTE} fee_config_refresh_queue=true"
        fi
        if [[ "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
          PREPARED_INPUT_NOTE="${PREPARED_INPUT_NOTE} second_close_after_results=${ITER_QUEUE_BURST_SECOND_CLOSE_AFTER_RESULTS} multi_reconnect=true"
        fi
        if [[ "${ITER_MODE}" == "bam_fee_source_mix_queue_burst_reconnect" || "${ITER_MODE}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
          PREPARED_INPUT_NOTE="${PREPARED_INPUT_NOTE} normal_tpu_after_queue_close=true"
        fi
      else
        PREPARED_INPUT_LABEL="synthetic_queue_burst_transfer_txnctx"
        PREPARED_INPUT_NOTE="system_kind=transfer lamports=${BURST_LAMPORTS} transfer_count=${BURST_TRANSFER_COUNT} queue_burst=true batch_count=${ITER_QUEUE_BURST_BATCH_COUNT} max_schedule_slot=${ITER_QUEUE_BURST_MAX_SCHEDULE_SLOT} close_after_results=${ITER_QUEUE_BURST_CLOSE_AFTER_RESULTS} cu_limit=${BURST_CU_LIMIT}"
        if [[ "${ITER_MODE}" == "quarantine_disable_enable_queue_inflight" || "${ITER_MODE}" == "quarantine_url_churn_queue_inflight" ]]; then
          PREPARED_INPUT_NOTE="${PREPARED_INPUT_NOTE} quarantine_generation_churn=true lamports_step=1"
        fi
        if [[ "${ITER_MODE}" == "queue_burst_multi_reconnect" || "${ITER_MODE}" == "source_mix_queue_burst_multi_reconnect" ]]; then
          PREPARED_INPUT_NOTE="${PREPARED_INPUT_NOTE} second_close_after_results=${ITER_QUEUE_BURST_SECOND_CLOSE_AFTER_RESULTS} multi_reconnect=true"
        fi
        if [[ "${ITER_MODE}" == "source_mix_queue_burst_multi_reconnect" ]]; then
          PREPARED_INPUT_NOTE="${PREPARED_INPUT_NOTE} normal_tpu_after_queue_close=true"
        fi
      fi
    else
      prepare_input_source       "${ITER_INPUT_FAMILY}"       "${ITER_DIR}"       "${ITER_SEED}"       "${INPUT_PATH}"       "${GEN_OUT}"       "${LAMPORTS}"       "${CU_LIMIT}"       "${PROBE_OUT}"       "${PROBE_ERR}"
    fi

    if [[ "${ITER_MODE}" == "stale_slot_reject" || "${ITER_MODE}" == "mixed_stale_multi_batch" || "${ITER_MODE}" == "mixed_stale_reconnect" || "${ITER_MODE}" == "mixed_terminal_producers_reconnect" || ( "${ITER_MODE}" == "random_mixed_multi_batch" && $(random_mixed_token_count "${ITER_RANDOM_MIX_PATTERN}" stale) -gt 0 ) ]]; then
      WAITED_SLOT=$(wait_rpc_slot_at_least 1 60) || die "validator slot did not advance past genesis for stale-slot test; latest slot=${WAITED_SLOT}"
    fi
    BLOCKHASH=$(wait_rpc_ready "${ITER_RPC_READY_TIMEOUT_SECS}") || die "validator RPC did not provide a fresh blockhash at ${RPC_URL}"
    bridge_packet "${PREPARED_INPUT_PATH}" "${PACKET_ONE_PATH}" "${BRIDGE_ONE_OUT}" "${BLOCKHASH}" "${TO_GENESIS_ACCOUNT_ONE}"
    bridge_packet "${PREPARED_INPUT_PATH}" "${PACKET_TWO_PATH}" "${BRIDGE_TWO_OUT}" "${BLOCKHASH}" "${TO_GENESIS_ACCOUNT_TWO}"
    if [[ "${ITER_MODE}" == "disable_enable_tpu_release" ]] && (( NORMAL_TPU_MATRIX_COUNT > 0 )); then
      prepare_normal_tpu_coverage_matrix "${ITER_DIR}" "${BLOCKHASH}" "${NORMAL_TPU_MATRIX_COUNT}"
    fi
    if is_queue_burst_mode "${ITER_MODE}"; then
      if [[ "${ITER_MODE}" == "bam_fee_config_refresh_queue_burst" || "${ITER_MODE}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_refresh" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
        prepare_fee_config_refresh_queue_packets "${ITER_DIR}" "${PREPARED_INPUT_PATH}" "${BLOCKHASH}" "${PACKET_ONE_PATH}" "${BRIDGE_ONE_OUT}"
      else
        prepare_queue_burst_packets "${ITER_DIR}" "${PREPARED_INPUT_PATH}" "${BLOCKHASH}" "${PACKET_ONE_PATH}" "${PACKET_TWO_PATH}" "${BRIDGE_ONE_OUT}" "${BRIDGE_TWO_OUT}"
      fi
    fi
    if [[ "${ITER_MODE}" == "source_mix_queue_burst_reconnect" || "${ITER_MODE}" == "source_mix_queue_burst_multi_reconnect" || "${ITER_MODE}" == "bam_fee_source_mix_queue_burst_reconnect" || "${ITER_MODE}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
      NORMAL_TPU_TO_INDEX=$((TO_GENESIS_ACCOUNT_ONE + ITER_QUEUE_BURST_BATCH_COUNT + 1))
      bridge_packet "${PREPARED_INPUT_PATH}" "${NORMAL_TPU_PACKET_PATH}" "${NORMAL_TPU_BRIDGE_OUT}" "${BLOCKHASH}" "${NORMAL_TPU_TO_INDEX}"
    fi

    ADAPT_ONE=$(grep '^adapted family=' "${BRIDGE_ONE_OUT}" || true)
    ADAPT_TWO=$(grep '^adapted family=' "${BRIDGE_TWO_OUT}" || true)
    [[ -n "${ADAPT_ONE}" && -n "${ADAPT_TWO}" ]] || die "failed to capture adapted system summaries"

    ITER_SYSTEM_KIND=$(extract_bridge_field system_kind "${ADAPT_ONE}")
    FROM_ONE=$(extract_bridge_field from "${ADAPT_ONE}")
    TO_ONE=$(extract_bridge_field to "${ADAPT_ONE}")
    LAMPORTS_ONE=$(extract_bridge_field lamports "${ADAPT_ONE}")
    ITER_OWNER_ONE=$(extract_bridge_field owner "${ADAPT_ONE}")
    ITER_SPACE_ONE=$(extract_bridge_field space "${ADAPT_ONE}")
    SYSTEM_KIND_TWO=$(extract_bridge_field system_kind "${ADAPT_TWO}")
    FROM_TWO=$(extract_bridge_field from "${ADAPT_TWO}")
    TO_TWO=$(extract_bridge_field to "${ADAPT_TWO}")
    LAMPORTS_TWO=$(extract_bridge_field lamports "${ADAPT_TWO}")
    ITER_OWNER_TWO=$(extract_bridge_field owner "${ADAPT_TWO}")
    ITER_SPACE_TWO=$(extract_bridge_field space "${ADAPT_TWO}")
    [[ -n "${ITER_SYSTEM_KIND}" && -n "${FROM_ONE}" && -n "${TO_ONE}" && -n "${LAMPORTS_ONE}" ]] || die "failed to parse first adapted system transaction"
    [[ -n "${SYSTEM_KIND_TWO}" && -n "${FROM_TWO}" && -n "${TO_TWO}" && -n "${LAMPORTS_TWO}" ]] || die "failed to parse second adapted system transaction"
    [[ "${FROM_ONE}" == "${FROM_TWO}" ]] || die "expected both adapted system transactions to share the same payer"
    [[ "${ITER_SYSTEM_KIND}" == "${SYSTEM_KIND_TWO}" ]] || die "expected both adapted packets to use the same system family"
    case "${ITER_SYSTEM_KIND}" in
      assign|allocate|create_account|create_account_with_seed)
        ;;
      *)
        ITER_OWNER_ONE=""
        ITER_SPACE_ONE=""
        ITER_OWNER_TWO=""
        ITER_SPACE_TWO=""
        ;;
    esac
    if [[ "${ITER_MODE}" == "stale_slot_reject" || "${ITER_MODE}" == "stale_slot_reject_reconnect" || "${ITER_MODE}" == "empty_batch_reject" || "${ITER_MODE}" == "empty_batch_reject_reconnect" || "${ITER_MODE}" == "malformed_first_atomic" || "${ITER_MODE}" == "malformed_first_atomic_reconnect" || "${ITER_MODE}" == "malformed_tail_atomic" || "${ITER_MODE}" == "malformed_tail_atomic_reconnect" || "${ITER_MODE}" == "bad_signature_first_atomic" || "${ITER_MODE}" == "bad_signature_first_atomic_reconnect" || "${ITER_MODE}" == "bad_signature_tail_atomic" || "${ITER_MODE}" == "bad_signature_tail_atomic_reconnect" || "${ITER_MODE}" == "non_atomic_valid_multi_packet" || "${ITER_MODE}" == "bam_cu_limit_fail" || "${ITER_MODE}" == "bam_cu_limit_fail_reconnect" ]]; then
      ITER_OWNER_ONE=""
      ITER_SPACE_ONE=""
      ITER_OWNER_TWO=""
      ITER_SPACE_TWO=""
    fi
    case "${ITER_MODE}" in
      unique_after_reconnect|seq_id_wrap_sequence|seq_id_out_of_order_multi_batch|seq_id_wrap_out_of_order_multi_batch|disable_enable_unique_after_reconnect|url_churn_unique_after_reconnect|url_sni_churn|bam_fee_url_churn_priority_commit|bam_fee_url_churn_same_slot_priority_commit|bam_fee_config_refresh_priority_commit|bam_fee_config_refresh_queue_burst|bam_fee_config_refresh_source_mix_queue_burst|bam_fee_config_midqueue_refresh|bam_fee_config_midqueue_source_mix_queue_burst|bam_fee_config_midqueue_source_mix_multi_reconnect|mixed_multi_batch|mixed_empty_multi_batch|mixed_malformed_multi_batch|mixed_bad_signature_multi_batch|mixed_bad_signature_reconnect|mixed_stale_multi_batch|mixed_stale_reconnect|mixed_terminal_producers_reconnect|random_mixed_multi_batch)
        ;;
      *)
        ITER_OWNER_TWO=""
        ITER_SPACE_TWO=""
        ;;
    esac

    ITER_FROM="${FROM_ONE}"
    ITER_PAYER_INITIAL=$(rpc_balance "${ITER_FROM}" 2>/dev/null || true)
    ITER_TO_ONE="${TO_ONE}"
    ITER_TO_TWO="${TO_TWO}"
    ITER_INITIAL_TO_ONE=$(rpc_balance "${ITER_TO_ONE}")
    ITER_INITIAL_TO_TWO=$(rpc_balance "${ITER_TO_TWO}")
    case "${ITER_MODE}" in
      partial_drain_reconnect|queue_burst_reconnect|queue_burst64_reconnect|queue_burst64_leader_plus1_reconnect|schedule_boundary_jitter|queue_reconnect_timing_jitter|queue_burst_multi_reconnect|source_mix_queue_burst_multi_reconnect|queue_burst128_reconnect|queue_burst256_reconnect|queue_burst512_reconnect|queue_burst_leader_reconnect|queue_burst64_leader_reconnect|bam_fee_queue_burst_reconnect|bam_fee_source_mix_queue_burst_reconnect|bam_fee_config_refresh_queue_burst|bam_fee_config_refresh_source_mix_queue_burst|bam_fee_config_midqueue_refresh|bam_fee_config_midqueue_source_mix_queue_burst|bam_fee_config_midqueue_source_mix_multi_reconnect|source_mix_queue_burst_reconnect|disable_enable_queue_burst_reconnect|quarantine_disable_enable_queue_inflight|quarantine_url_churn_queue_inflight|non_atomic_inconsistent_bundle|non_atomic_first_overdraft|non_atomic_mid_overdraft|non_atomic_partial_overdraft|non_atomic_partial_overdraft_reconnect|non_atomic_partial_resolver_fail|non_atomic_partial_blockhash_fail|non_atomic_partial_duplicate_sig|non_atomic_partial_cu_fail)
        ITER_EXPECT_TO_ONE=""
        ;;
      stale_slot_reject|stale_slot_reject_reconnect|empty_batch_reject|empty_batch_reject_reconnect|malformed_first_atomic|malformed_first_atomic_reconnect|malformed_tail_atomic|malformed_tail_atomic_reconnect|bad_signature_first_atomic|bad_signature_first_atomic_reconnect|bad_signature_tail_atomic|bad_signature_tail_atomic_reconnect|bam_cu_limit_fail|bam_cu_limit_fail_reconnect)
        ITER_EXPECT_TO_ONE="${ITER_INITIAL_TO_ONE}"
        ;;
      *)
        ITER_EXPECT_TO_ONE=$((ITER_INITIAL_TO_ONE + LAMPORTS_ONE))
        ;;
    esac
    case "${ITER_MODE}" in
      partial_drain_reconnect|queue_burst_reconnect|queue_burst64_reconnect|queue_burst64_leader_plus1_reconnect|schedule_boundary_jitter|queue_reconnect_timing_jitter|queue_burst_multi_reconnect|source_mix_queue_burst_multi_reconnect|queue_burst128_reconnect|queue_burst256_reconnect|queue_burst512_reconnect|queue_burst_leader_reconnect|queue_burst64_leader_reconnect|bam_fee_queue_burst_reconnect|bam_fee_source_mix_queue_burst_reconnect|bam_fee_config_refresh_queue_burst|bam_fee_config_refresh_source_mix_queue_burst|bam_fee_config_midqueue_refresh|bam_fee_config_midqueue_source_mix_queue_burst|bam_fee_config_midqueue_source_mix_multi_reconnect|source_mix_queue_burst_reconnect|disable_enable_queue_burst_reconnect|quarantine_disable_enable_queue_inflight|quarantine_url_churn_queue_inflight|non_atomic_inconsistent_bundle|non_atomic_first_overdraft|non_atomic_mid_overdraft|non_atomic_partial_overdraft|non_atomic_partial_overdraft_reconnect|non_atomic_partial_resolver_fail|non_atomic_partial_blockhash_fail|non_atomic_partial_duplicate_sig|non_atomic_partial_cu_fail)
        ITER_EXPECT_TO_TWO=""
        ;;
      unique_after_reconnect|seq_id_wrap_sequence|seq_id_out_of_order_multi_batch|seq_id_wrap_out_of_order_multi_batch|disable_enable_unique_after_reconnect|disable_enable_tpu_release|url_churn_unique_after_reconnect|url_sni_churn|bam_fee_url_churn_priority_commit|bam_fee_url_churn_same_slot_priority_commit|bam_fee_config_refresh_priority_commit|mixed_multi_batch|mixed_empty_multi_batch|mixed_malformed_multi_batch|mixed_bad_signature_multi_batch|mixed_bad_signature_reconnect|mixed_stale_multi_batch|mixed_stale_reconnect|mixed_terminal_producers_reconnect|random_mixed_multi_batch|non_atomic_valid_multi_packet)
        ITER_EXPECT_TO_TWO=$((ITER_INITIAL_TO_TWO + LAMPORTS_TWO))
        ;;
      *)
        ITER_EXPECT_TO_TWO="${ITER_INITIAL_TO_TWO}"
        ;;
    esac
    if [[ "${ITER_MODE}" == "bam_fee_priority_commit" ]]; then
      CURRENT_BAM_BUILDER_PUBKEY="${ITER_TO_TWO}"
      ITER_BAM_FEE_RECIPIENT="${ITER_TO_TWO}"
      ITER_BAM_FEE_RECIPIENT_INITIAL="${ITER_INITIAL_TO_TWO}"
      ITER_EXPECT_TO_TWO=""
    elif [[ "${ITER_MODE}" == "bam_fee_priority_replay_after_reconnect" ]]; then
      CURRENT_BAM_BUILDER_PUBKEY="${ITER_TO_ONE}"
      ITER_BAM_FEE_RECIPIENT="${ITER_TO_ONE}"
      ITER_BAM_FEE_RECIPIENT_INITIAL="${ITER_INITIAL_TO_ONE}"
    fi
    if [[ "${ITER_MODE}" == "bam_fee_queue_burst_reconnect" || "${ITER_MODE}" == "bam_fee_source_mix_queue_burst_reconnect" || "${ITER_MODE}" == "bam_fee_config_refresh_queue_burst" || "${ITER_MODE}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_refresh" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
      CURRENT_BAM_BUILDER_PUBKEY="${ITER_TO_TWO}"
      ITER_BAM_FEE_RECIPIENT="${ITER_TO_TWO}"
      ITER_BAM_FEE_RECIPIENT_INITIAL="${ITER_INITIAL_TO_TWO}"
      ITER_EXPECT_TO_ONE=""
      ITER_EXPECT_TO_TWO=""
    fi
    if [[ "${ITER_MODE}" == "bam_fee_config_commission_refresh_priority_commit" ]]; then
      CURRENT_BAM_BUILDER_PUBKEY="${ITER_TO_TWO}"
      ITER_BAM_FEE_RECIPIENT="${ITER_TO_TWO}"
      ITER_BAM_FEE_RECIPIENT_INITIAL="${ITER_INITIAL_TO_TWO}"
      CURRENT_BAM_OMIT_BLOCK_ENGINE_CONFIG=1
      ITER_EXPECT_TO_TWO=""
    fi
    if [[ ( "${ITER_MODE}" == "bam_fee_url_churn_priority_commit" || "${ITER_MODE}" == "bam_fee_url_churn_same_slot_priority_commit" || "${ITER_MODE}" == "bam_fee_config_refresh_priority_commit" || "${ITER_MODE}" == "bam_fee_config_refresh_queue_burst" || "${ITER_MODE}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_refresh" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ) ]]; then
      CURRENT_BAM_BUILDER_PUBKEY="${ITER_TO_TWO}"
      ITER_BAM_FEE_RECIPIENT="${ITER_TO_TWO}"
      ITER_BAM_FEE_RECIPIENT_INITIAL="${ITER_INITIAL_TO_TWO}"
      ITER_BAM_FEE_RECIPIENT_SECOND="${ITER_TO_ONE}"
      ITER_BAM_FEE_RECIPIENT_SECOND_INITIAL="${ITER_INITIAL_TO_ONE}"
      ITER_BAM_FEE_CHURN_TRANSFER_ONE="${LAMPORTS_ONE}"
      ITER_BAM_FEE_CHURN_TRANSFER_TWO="${LAMPORTS_TWO}"
      if [[ "${ITER_MODE}" == "bam_fee_config_refresh_priority_commit" || "${ITER_MODE}" == "bam_fee_config_refresh_queue_burst" || "${ITER_MODE}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_refresh" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
        CURRENT_BAM_OMIT_BLOCK_ENGINE_CONFIG=1
      fi
      ITER_EXPECT_TO_ONE=""
      ITER_EXPECT_TO_TWO=""
    fi

	    if [[ "${ITER_MODE}" == "non_atomic_valid_multi_packet" ]]; then
      cat "${PACKET_ONE_PATH}" "${PACKET_TWO_PATH}" >"${PACKET_BATCH_PATH}"
    elif [[ "${ITER_MODE}" == "malformed_first_atomic" || "${ITER_MODE}" == "malformed_first_atomic_reconnect" ]]; then
      {
        printf '%s\n' 'AA=='
        cat "${PACKET_TWO_PATH}"
      } >"${PACKET_BATCH_PATH}"
    elif [[ "${ITER_MODE}" == "malformed_tail_atomic" || "${ITER_MODE}" == "malformed_tail_atomic_reconnect" ]]; then
      {
        cat "${PACKET_ONE_PATH}"
        printf '%s\n' 'AA=='
      } >"${PACKET_BATCH_PATH}"
    elif [[ "${ITER_MODE}" == "bad_signature_first_atomic" || "${ITER_MODE}" == "bad_signature_first_atomic_reconnect" ]]; then
      BAD_SIGNATURE_PACKET_PATH="${ITER_DIR}/packet_bad_signature.txt"
      write_bad_signature_packet "${PACKET_ONE_PATH}" "${BAD_SIGNATURE_PACKET_PATH}"
      cat "${BAD_SIGNATURE_PACKET_PATH}" "${PACKET_TWO_PATH}" >"${PACKET_BATCH_PATH}"
	    elif [[ "${ITER_MODE}" == "bad_signature_tail_atomic" || "${ITER_MODE}" == "bad_signature_tail_atomic_reconnect" ]]; then
	      BAD_SIGNATURE_PACKET_PATH="${ITER_DIR}/packet_bad_signature.txt"
	      write_bad_signature_packet "${PACKET_TWO_PATH}" "${BAD_SIGNATURE_PACKET_PATH}"
	      cat "${PACKET_ONE_PATH}" "${BAD_SIGNATURE_PACKET_PATH}" >"${PACKET_BATCH_PATH}"
	    elif [[ "${ITER_MODE}" == "mixed_malformed_multi_batch" ]]; then
	      printf '%s\n' 'AA==' >"${PACKET_BATCH_PATH}"
	    elif [[ "${ITER_MODE}" == "mixed_bad_signature_multi_batch" || "${ITER_MODE}" == "mixed_bad_signature_reconnect" ]]; then
	      write_bad_signature_packet "${PACKET_ONE_PATH}" "${PACKET_BATCH_PATH}"
	    fi
  fi

  echo "  blockhash: ${BLOCKHASH}"
  echo "  prepared input: ${PREPARED_INPUT_PATH}"
  echo "  input label: ${PREPARED_INPUT_LABEL}"
  echo "  input note: ${PREPARED_INPUT_NOTE}"
  if [[ "${ITER_MODE}" == "vote_reject_once" || "${ITER_MODE}" == "vote_reject_reconnect" ]]; then
    echo "  system kind: ${ITER_SYSTEM_KIND}"
    echo "  vote node: ${ITER_SIMPLE_VOTE_NODE}"
    echo "  vote account: ${ITER_VOTE_ACCOUNT}"
    echo "  authorized voter: ${ITER_AUTHORIZED_VOTER}"
    echo "  vote slot/hash: ${ITER_VOTE_SLOT} / ${ITER_VOTE_HASH}"
  else
    echo "  system kind: ${ITER_SYSTEM_KIND:-n/a}"
    if [[ -n "${ITER_RANDOM_MIX_PATTERN}" ]]; then
      echo "  random mixed pattern: ${ITER_RANDOM_MIX_PATTERN}"
    fi
    echo "  payer: ${ITER_FROM}"
    echo "  recipient one: ${ITER_TO_ONE} pre=${ITER_INITIAL_TO_ONE} expected=${ITER_EXPECT_TO_ONE}"
    echo "  recipient two: ${ITER_TO_TWO} pre=${ITER_INITIAL_TO_TWO} expected=${ITER_EXPECT_TO_TWO}"
    if [[ -n "${ITER_DURABLE_NONCE_ACCOUNT}" ]]; then
      echo "  durable nonce: account=${ITER_DURABLE_NONCE_ACCOUNT} hash=${ITER_DURABLE_NONCE_HASH}"
    fi
  fi

	  if [[ "${ITER_MODE}" != "external_scenario" && "${ITER_MODE}" != "generated_bundle" ]]; then
	    render_scenario "${ITER_MODE}" "${SCENARIO_PATH}" "${PACKET_ONE_PATH}" "${PACKET_TWO_PATH}" "${PACKET_BATCH_PATH}" "${SEQ_ONE}" "${SEQ_TWO}" "${ITER_SEED}"
	  fi
	  ITER_START_SLOT=$(rpc_slot 2>/dev/null || printf '0')
	  if [[ ( "${ITER_MODE}" == "bam_fee_url_churn_priority_commit" || "${ITER_MODE}" == "bam_fee_url_churn_same_slot_priority_commit" ) ]]; then
	    ITER_BAM_SECONDARY_LOG="${ITER_DIR}/bam-secondary.log"
	    SECONDARY_SCENARIO_PATH="${ITER_DIR}/scenario-secondary.toml"
	    render_bam_fee_url_churn_second_scenario "${SECONDARY_SCENARIO_PATH}" "${PACKET_TWO_PATH}" "${SEQ_TWO}"
	    start_bam_controller_at \
	      "${ITER_DIR}" \
	      "bam-secondary" \
	      "${ITER_BAM_SECONDARY_LOG}" \
	      "${SECONDARY_SCENARIO_PATH}" \
	      "$(url_bind_addr "${BAM_BAD_URL}")" \
	      "${ITER_TO_ONE}" \
	      1
	    wait_for_pattern "${ITER_BAM_SECONDARY_LOG}" '^BAM test server listening on ' 15 \
	      || die "secondary BAM fee-config server did not start at ${BAM_BAD_URL}"
	  fi
	  start_bam_controller "${ITER_DIR}" "${BAM_LOG}" "${SCENARIO_PATH}"

	  if [[ -n "${EXTERNAL_OPERATOR_EVENTS_FILE}" ]]; then
	    materialize_operator_events "${EXTERNAL_OPERATOR_EVENTS_FILE}" "${EVENTS_PATH}" "${ITER_DIR}"
	    start_operator_runner "${EVENTS_PATH}" "${FD_LOG}" "${BAM_LOG}" "${OPERATOR_LOG}"
	  elif render_operator_events "${ITER_MODE}" "${EVENTS_PATH}" "${SEQ_ONE}"; then
	    start_operator_runner "${EVENTS_PATH}" "${FD_LOG}" "${BAM_LOG}" "${OPERATOR_LOG}"
	  fi

	  if [[ "${ITER_MODE}" == "source_mix_bam_tpu" ]]; then
	    send_normal_tpu_packet_after_bam_commit "${PACKET_TWO_PATH}" "${BAM_LOG}" "${SEQ_ONE}" "${NORMAL_TPU_LOG}"
	  elif [[ "${ITER_MODE}" == "source_mix_precommit" ]]; then
	    send_normal_tpu_packet_after_scheduler_ready "${PACKET_TWO_PATH}" "${BAM_LOG}" "${NORMAL_TPU_LOG}"
	  elif [[ "${ITER_MODE}" == "source_mix_atomic_revert_precommit" ]]; then
	    send_normal_tpu_packet_after_scheduler_ready "${PACKET_ONE_PATH}" "${BAM_LOG}" "${NORMAL_TPU_LOG}"
	  elif [[ "${ITER_MODE}" == "source_mix_duplicate_tpu_after_bam" ]]; then
	    send_normal_tpu_packet_after_bam_commit "${PACKET_ONE_PATH}" "${BAM_LOG}" "${SEQ_ONE}" "${NORMAL_TPU_LOG}"
	  elif [[ "${ITER_MODE}" == "disable_enable_tpu_release" ]]; then
	    send_normal_tpu_packet_after_bam_disable "${PACKET_TWO_PATH}" "${BAM_LOG}" "${OPERATOR_LOG}" "${NORMAL_TPU_LOG}"
	    ITER_NORMAL_TPU_EXPECTED_LANDED="true"
	  elif [[ "${ITER_MODE}" == "source_mix_queue_burst_reconnect" || "${ITER_MODE}" == "source_mix_queue_burst_multi_reconnect" || "${ITER_MODE}" == "bam_fee_source_mix_queue_burst_reconnect" || "${ITER_MODE}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
	    send_normal_tpu_packet_after_queue_close "${NORMAL_TPU_PACKET_PATH}" "${BAM_LOG}" "${NORMAL_TPU_LOG}"
	  fi

		  validate_iteration     "${ITER_MODE}"     "${SEQ_ONE}"     "${SEQ_TWO}"     "${FD_LOG}"     "${BAM_LOG}"     "${OPERATOR_LOG}"     "${ITER_TO_ONE}"     "${ITER_INITIAL_TO_ONE}"     "${ITER_EXPECT_TO_ONE}"     "${ITER_TO_TWO}"     "${ITER_INITIAL_TO_TWO}"     "${ITER_EXPECT_TO_TWO}"
	  if [[ "${ITER_MODE}" == "generated_bundle" ]]; then
	    sleep 2
	  fi
	  if [[ -n "${ITER_FROM}" && "${ITER_FROM}" != "n/a" ]]; then
	    ITER_PAYER_OBSERVED=$(rpc_balance "${ITER_FROM}" 2>/dev/null || true)
	  fi
	  if [[ "${ITER_MODE}" == "generated_bundle" && -n "${ITER_TO_ONE}" && "${ITER_TO_ONE}" != "n/a" ]]; then
	    ITER_OBSERVED_TO_ONE=$(rpc_balance "${ITER_TO_ONE}" 2>/dev/null || true)
	    ITER_OWNER_ONE_OBSERVED=$(rpc_account_owner "${ITER_TO_ONE}" 2>/dev/null || true)
	    ITER_SPACE_ONE_OBSERVED=$(rpc_account_space "${ITER_TO_ONE}" 2>/dev/null || true)
	  fi
	  if [[ "${ITER_MODE}" == "generated_bundle" && -n "${ITER_TO_TWO}" && "${ITER_TO_TWO}" != "n/a" ]]; then
	    ITER_OBSERVED_TO_TWO=$(rpc_balance "${ITER_TO_TWO}" 2>/dev/null || true)
	    ITER_OWNER_TWO_OBSERVED=$(rpc_account_owner "${ITER_TO_TWO}" 2>/dev/null || true)
	    ITER_SPACE_TWO_OBSERVED=$(rpc_account_space "${ITER_TO_TWO}" 2>/dev/null || true)
	  fi
	  ITER_END_SLOT=$(rpc_slot 2>/dev/null || printf "${ITER_START_SLOT}")
	  capture_rpc_blocks "${ITER_START_SLOT}" "${ITER_END_SLOT}" "${ITER_DIR}/rpc_blocks.jsonl"
	  if [[ "${ITER_MODE}" == "source_mix_duplicate_tpu_after_bam" || "${ITER_MODE}" == "source_mix_atomic_revert_precommit" ]]; then
	    capture_rpc_signature_statuses "${SCENARIO_PATH}" "${ITER_DIR}/rpc_signature_statuses.jsonl" "${PACKET_ONE_PATH}"
	  elif [[ "${ITER_MODE}" == "source_mix_bam_tpu" || "${ITER_MODE}" == "source_mix_precommit" || "${ITER_MODE}" == "disable_enable_tpu_release" ]]; then
	    capture_rpc_signature_statuses "${SCENARIO_PATH}" "${ITER_DIR}/rpc_signature_statuses.jsonl" "${PACKET_TWO_PATH}"
	  elif [[ "${ITER_MODE}" == "source_mix_queue_burst_reconnect" || "${ITER_MODE}" == "source_mix_queue_burst_multi_reconnect" || "${ITER_MODE}" == "bam_fee_source_mix_queue_burst_reconnect" || "${ITER_MODE}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
	    capture_rpc_signature_statuses "${SCENARIO_PATH}" "${ITER_DIR}/rpc_signature_statuses.jsonl" "${NORMAL_TPU_PACKET_PATH}"
	  else
	    capture_rpc_signature_statuses "${SCENARIO_PATH}" "${ITER_DIR}/rpc_signature_statuses.jsonl"
	  fi

	  if [[ "${CHECK_BAM_SHRED}" == "1" ]]; then
	    stop_bam_shred_capture "${BAM_SHRED_CAPTURE_PATH}"
	    ITER_BAM_SHRED_CAPTURE="${BAM_SHRED_CAPTURE_PATH}"
	    IFS=' ' read -r ITER_BAM_SHRED_CAPTURE_PACKET_COUNT ITER_BAM_SHRED_CAPTURE_BYTE_COUNT < <(
	      python3 - "${BAM_SHRED_CAPTURE_PATH}" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as f:
    data = json.load(f)
print(int(data.get("packet_count", 0)), int(data.get("byte_count", 0)))
PY
	    )
	  fi

	  ITER_NORMAL_TPU_LOG=""
	  ITER_NORMAL_TPU_PACKET=""
	  ITER_NORMAL_TPU_PORT=""
	  if [[ "${ITER_MODE}" == "source_mix_bam_tpu" || "${ITER_MODE}" == "source_mix_precommit" || "${ITER_MODE}" == "source_mix_atomic_revert_precommit" || "${ITER_MODE}" == "source_mix_duplicate_tpu_after_bam" || "${ITER_MODE}" == "disable_enable_tpu_release" ]]; then
	    ITER_NORMAL_TPU_LOG="${NORMAL_TPU_LOG}"
	    if [[ "${ITER_MODE}" == "source_mix_duplicate_tpu_after_bam" || "${ITER_MODE}" == "source_mix_atomic_revert_precommit" ]]; then
	      ITER_NORMAL_TPU_PACKET="${PACKET_ONE_PATH}"
	    else
	      ITER_NORMAL_TPU_PACKET="${PACKET_TWO_PATH}"
	    fi
	    ITER_NORMAL_TPU_PORT="${BAM_TPU_PORT}"
	  elif [[ "${ITER_MODE}" == "source_mix_queue_burst_reconnect" || "${ITER_MODE}" == "source_mix_queue_burst_multi_reconnect" || "${ITER_MODE}" == "bam_fee_source_mix_queue_burst_reconnect" || "${ITER_MODE}" == "bam_fee_config_refresh_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_queue_burst" || "${ITER_MODE}" == "bam_fee_config_midqueue_source_mix_multi_reconnect" ]]; then
	    ITER_NORMAL_TPU_LOG="${NORMAL_TPU_LOG}"
	    ITER_NORMAL_TPU_PACKET="${NORMAL_TPU_PACKET_PATH}"
	    ITER_NORMAL_TPU_PORT="${BAM_TPU_PORT}"
	  fi

	  if [[ "${ITER_MODE}" == "vote_reject_once" || "${ITER_MODE}" == "vote_reject_reconnect" ]]; then
	    cat > "${ITER_DIR}/summary.txt" <<EOF2
seed=${ITER_SEED}
mode=${ITER_MODE}
input_family=${ITER_INPUT_FAMILY}
start_slot=${ITER_START_SLOT}
end_slot=${ITER_END_SLOT}
scenario_file=${SCENARIO_PATH}
requested_kunorpus_system_kind=${KUNORPUS_SYSTEM_KIND}
kunorpus_count=${KUNORPUS_COUNT}
kunorpus_seed_window=${KUNORPUS_SEED_WINDOW}
kunorpus_max_transfer_lamports=${KUNORPUS_MAX_TRANSFER_LAMPORTS}
generated_bundle_max_batches=${GENERATED_BUNDLE_MAX_BATCHES}
generated_bundle_max_packets=${GENERATED_BUNDLE_MAX_PACKETS}
system_kind=${ITER_SYSTEM_KIND}
input_path=${PREPARED_INPUT_PATH}
input_label=${PREPARED_INPUT_LABEL}
input_note=${PREPARED_INPUT_NOTE}
seq_one=${SEQ_ONE}
seq_two=${SEQ_TWO}
seq_three=${SEQ_THREE}
vote_node=${ITER_SIMPLE_VOTE_NODE}
vote_account=${ITER_VOTE_ACCOUNT}
authorized_voter=${ITER_AUTHORIZED_VOTER}
vote_slot=${ITER_VOTE_SLOT}
vote_hash=${ITER_VOTE_HASH}
pre_authorized_voter=${ITER_PRE_AUTHORIZED_VOTER}
pre_votes_len=${ITER_PRE_VOTES_LEN}
pre_last_timestamp_slot=${ITER_PRE_LAST_TIMESTAMP_SLOT}
vote_account_json_unsupported=${ITER_VOTE_ACCOUNT_JSON_UNSUPPORTED}
post_authorized_voter=${ITER_POST_AUTHORIZED_VOTER}
post_votes_len=${ITER_POST_VOTES_LEN}
post_last_timestamp_slot=${ITER_POST_LAST_TIMESTAMP_SLOT}
bam_shred_capture=${ITER_BAM_SHRED_CAPTURE}
bam_shred_capture_packet_count=${ITER_BAM_SHRED_CAPTURE_PACKET_COUNT}
bam_shred_capture_byte_count=${ITER_BAM_SHRED_CAPTURE_BYTE_COUNT}
vote_account_history_may_advance=true
vote_account_history_note=fddev normal tower voting may update the validator vote account while the BAM vote rejection is being tested
EOF2
	  else
	    cat > "${ITER_DIR}/summary.txt" <<EOF2
seed=${ITER_SEED}
mode=${ITER_MODE}
input_family=${ITER_INPUT_FAMILY}
start_slot=${ITER_START_SLOT}
end_slot=${ITER_END_SLOT}
scenario_file=${SCENARIO_PATH}
requested_kunorpus_system_kind=${KUNORPUS_SYSTEM_KIND}
kunorpus_count=${KUNORPUS_COUNT}
kunorpus_seed_window=${KUNORPUS_SEED_WINDOW}
kunorpus_max_transfer_lamports=${KUNORPUS_MAX_TRANSFER_LAMPORTS}
generated_bundle_max_batches=${GENERATED_BUNDLE_MAX_BATCHES}
generated_bundle_max_packets=${GENERATED_BUNDLE_MAX_PACKETS}
system_kind=${ITER_SYSTEM_KIND}
input_path=${PREPARED_INPUT_PATH}
input_label=${PREPARED_INPUT_LABEL}
input_note=${PREPARED_INPUT_NOTE}
random_mixed_pattern=${ITER_RANDOM_MIX_PATTERN}
seq_one=${SEQ_ONE}
seq_two=${SEQ_TWO}
seq_three=${SEQ_THREE}
valid_alt_lookup_table=${VALID_ALT_LOOKUP_TABLE}
durable_nonce_account=${ITER_DURABLE_NONCE_ACCOUNT}
durable_nonce_hash=${ITER_DURABLE_NONCE_HASH}
payer=${ITER_FROM}
payer_initial=${ITER_PAYER_INITIAL}
payer_observed=${ITER_PAYER_OBSERVED}
recipient_one=${ITER_TO_ONE}
recipient_one_initial=${ITER_INITIAL_TO_ONE}
recipient_one_expected=${ITER_EXPECT_TO_ONE}
recipient_one_observed=${ITER_OBSERVED_TO_ONE}
recipient_one_owner_expected=${ITER_OWNER_ONE}
recipient_one_owner_observed=${ITER_OWNER_ONE_OBSERVED}
recipient_one_space_expected=${ITER_SPACE_ONE}
recipient_one_space_observed=${ITER_SPACE_ONE_OBSERVED}
recipient_two=${ITER_TO_TWO}
recipient_two_initial=${ITER_INITIAL_TO_TWO}
recipient_two_expected=${ITER_EXPECT_TO_TWO}
recipient_two_observed=${ITER_OBSERVED_TO_TWO}
recipient_two_owner_expected=${ITER_OWNER_TWO}
recipient_two_owner_observed=${ITER_OWNER_TWO_OBSERVED}
recipient_two_space_expected=${ITER_SPACE_TWO}
recipient_two_space_observed=${ITER_SPACE_TWO_OBSERVED}
bam_fee_recipient=${ITER_BAM_FEE_RECIPIENT}
bam_fee_recipient_initial=${ITER_BAM_FEE_RECIPIENT_INITIAL}
bam_fee_recipient_observed=${ITER_BAM_FEE_RECIPIENT_OBSERVED}
bam_fee_recipient_second=${ITER_BAM_FEE_RECIPIENT_SECOND}
bam_fee_recipient_second_initial=${ITER_BAM_FEE_RECIPIENT_SECOND_INITIAL}
bam_fee_recipient_second_observed=${ITER_BAM_FEE_RECIPIENT_SECOND_OBSERVED}
bam_fee_churn_transfer_one=${ITER_BAM_FEE_CHURN_TRANSFER_ONE}
bam_fee_churn_transfer_two=${ITER_BAM_FEE_CHURN_TRANSFER_TWO}
bam_fee_secondary_log=${ITER_BAM_SECONDARY_LOG}
bam_fee_recipient_expected_min_delta=0
bam_fee_commission_bps=${BAM_COMMISSION_BPS}
bam_fee_builder_commission_pct=${BAM_BUILDER_COMMISSION_PCT}
normal_tpu_log=${ITER_NORMAL_TPU_LOG}
normal_tpu_packet=${ITER_NORMAL_TPU_PACKET}
normal_tpu_port=${ITER_NORMAL_TPU_PORT}
normal_tpu_expected_landed=${ITER_NORMAL_TPU_EXPECTED_LANDED}
bam_shred_capture=${ITER_BAM_SHRED_CAPTURE}
bam_shred_capture_packet_count=${ITER_BAM_SHRED_CAPTURE_PACKET_COUNT}
bam_shred_capture_byte_count=${ITER_BAM_SHRED_CAPTURE_BYTE_COUNT}
EOF2
  fi

  write_normalized_outcome "${ITER_DIR}" "${ITER_MODE}" "${ITER_INPUT_FAMILY}" "${RUNNER_KIND}"
  run_bam_disabled_workload "${ITER_DIR}"
  echo "  iteration passed; artifacts: ${ITER_DIR}"
done

echo
echo "validated: seeded live BAM stateful runner completed ${ITERATIONS} iteration(s) on ${TARGET_NAME}"
echo "preserved self-check failures: ${SELF_CHECK_FAILURES}"
