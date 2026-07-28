#!/usr/bin/env bash

# Differentially replay fddev BAM iterations against the Jito Agave BAM client.
#
# The fddev side is produced by contrib/fuzz-local-bam-stateful.sh.  Each
# replayable iteration is then run through contrib/run-jito-agave-bam-scenario.sh
# and compared with contrib/compare-bam-outcomes.py.

set -euo pipefail
IFS=$'\n\t'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)

SOURCE_LOG_DIR=""
LOG_DIR=""
TARGET_PAIR="${TARGET_PAIR:-fddev:jito-agave}"
ITERATIONS=1
SEED=1
MODE="commit_once"
MODE_EXPLICIT=0
MODE_LIST=""
EXTERNAL_SCENARIO_FILE=""
INPUT_FAMILY="synthetic"
KUNORPUS_SYSTEM_KIND="any"
TIMEOUT_SECS=180
BUILD_JITO=0
JITO_SOLANA_DIR="${JITO_SOLANA_DIR:-${HOME}/jito-solana}"
JITO_PROFILE="${JITO_PROFILE:-release}"
JITO_BALANCE_OBSERVE_TIMEOUT_SECS="${JITO_BALANCE_OBSERVE_TIMEOUT_SECS:-20}"
XFAIL_KNOWN=0
KNOWN_FAILURES_FILE="${ROOT}/contrib/bam-diff-known-failures.tsv"
IGNORE_CUS_CONSUMED=0
IGNORE_BATCH_RESULT_ORDER=0
IGNORE_LOADED_ACCOUNTS_DATA_SIZE=0
IGNORE_CHAIN_EVIDENCE=0
ALLOW_OPERATOR_PERTURBATIONS=0
BASE_SWEEP=0
CONTROL_PLANE_SWEEP=0
PRE_FULLFD_HARDENING_SWEEP=0
EXPORT_SCENARIO_CORPUS=""
LIVE_COVERAGE_DIR=""
FDDEV_CONFIG="${FDDEV_CONFIG:-}"
FDDEV_CONFIG_TEMPLATE="${FDDEV_CONFIG_TEMPLATE:-${ROOT}/contrib/local-bam-compact.toml}"
FULLFD_CONFIG="${FULLFD_CONFIG:-}"
FULLFD_CONFIG_TEMPLATE="${FULLFD_CONFIG_TEMPLATE:-${ROOT}/contrib/local-bam-fullfd-compact.toml}"
FULLFD_BIN="${FULLFD_BIN:-${ROOT}/build/native/gcc/bin/firedancer-dev}"
FULLFD_SNAPSHOT_FILE="${FULLFD_SNAPSHOT_FILE:-}"
FULLFD_SNAPSHOT_GENESIS_FILE="${FULLFD_SNAPSHOT_GENESIS_FILE:-}"
FULLFD_SNAPSHOT_SHRED_VERSION="${FULLFD_SNAPSHOT_SHRED_VERSION:-}"
GUI_PORT="${GUI_PORT:-}"
RPC_PORT="${RPC_PORT:-8899}"
JITO_RPC_PORT="${JITO_RPC_PORT:-}"
FDDEV_RPC_URL="${FDDEV_RPC_URL:-}"
BAM_BIND="${BAM_BIND:-127.0.0.1:50055}"
BAM_URL="${BAM_URL:-http://127.0.0.1:50055}"
BAM_BAD_URL="${BAM_BAD_URL:-http://127.0.0.1:50056}"
BAM_TPU_PORT="${BAM_TPU_PORT:-9007}"
BAM_TPU_FWD_PORT="${BAM_TPU_FWD_PORT:-9008}"
BAM_SHRED_PORT="${BAM_SHRED_PORT:-9009}"
CHECK_BAM_SHRED="${CHECK_BAM_SHRED:-0}"
QUEUE_BURST_BATCH_COUNT="${QUEUE_BURST_BATCH_COUNT:-}"
QUEUE_BURST_MAX_SCHEDULE_SLOT="${QUEUE_BURST_MAX_SCHEDULE_SLOT:-}"
QUEUE_BURST_CLOSE_AFTER_RESULTS="${QUEUE_BURST_CLOSE_AFTER_RESULTS:-}"
FAUCET_PORT="${FAUCET_PORT:-9900}"
FAUCET_PORT_EXPLICIT=0
ISOLATED_PORT_BASE=""
LONG_SLOT_HASHES_PER_TICK="${LONG_SLOT_HASHES_PER_TICK:-62500}"
LONG_SLOT_TICKS_PER_SLOT="${LONG_SLOT_TICKS_PER_SLOT:-4096}"
LONG_SLOT_TARGET_TICK_DURATION_MICROS="${LONG_SLOT_TARGET_TICK_DURATION_MICROS:-6250}"
JITO_TICKS_PER_SLOT=""


mode_selection_contains() {
  local needle="$1"
  if [[ "${MODE}" == "${needle}" ]]; then
    return 0
  fi
  if [[ -n "${MODE_LIST}" && ",${MODE_LIST}," == *",${needle},"* ]]; then
    return 0
  fi
  return 1
}

choose_free_tcp_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

BASE_SWEEP_MODES=(
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
  source_mix_precommit
  source_mix_atomic_revert_precommit
  source_mix_duplicate_tpu_after_bam
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
  stale_slot_reject
  empty_batch_reject
  empty_batch_reject_reconnect
  malformed_tail_atomic
  bad_signature_tail_atomic
  non_atomic_single_packet
  bam_fee_priority_commit
  bam_fee_priority_replay_after_reconnect
  bam_fee_queue_burst_reconnect
  bam_fee_source_mix_queue_burst_reconnect
  bam_fee_url_churn_priority_commit
  bam_fee_config_refresh_priority_commit
  bam_fee_config_commission_refresh_priority_commit
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
  invalid_alt_missing_table
  invalid_alt_missing_table_reconnect
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
  vote_reject_once
  vote_reject_reconnect
  raw_kunorpus_once
  raw_kunorpus_reconnect
)

CONTROL_PLANE_SWEEP_MODES=(
  disable_enable_unique_after_reconnect
  url_churn_unique_after_reconnect
)

PRE_FULLFD_HARDENING_SWEEP_MODES=(
  commit_once
  replay_same_conn
  replay_after_reconnect
  unique_after_reconnect
  seq_id_max_once
  seq_id_max_replay_after_reconnect
  seq_collision_same_conn
  seq_collision_reconnect
  seq_id_wrap_sequence
  seq_id_out_of_order_multi_batch
  seq_id_wrap_out_of_order_multi_batch
  seq_id_wrap_conflicting_spend_multi_batch
  duplicate_seq_split
  duplicate_seq_split_reconnect
  partial_drain_reconnect
  queue_burst64_reconnect
  queue_burst128_reconnect
  queue_burst256_reconnect
  queue_burst512_reconnect
  queue_burst64_leader_reconnect
  queue_burst64_leader_plus1_reconnect
  queue_reconnect_timing_jitter
  queue_burst_multi_reconnect
  schedule_boundary_jitter
  disable_enable_unique_after_reconnect
  url_churn_unique_after_reconnect
  source_mix_bam_tpu
  source_mix_precommit
  source_mix_atomic_revert_precommit
  source_mix_duplicate_tpu_after_bam
  source_mix_queue_burst_reconnect
  source_mix_queue_burst_multi_reconnect
  disable_enable_queue_burst_reconnect
  mixed_multi_batch
  mixed_empty_multi_batch
  mixed_malformed_multi_batch
  mixed_bad_signature_multi_batch
  mixed_bad_signature_reconnect
  mixed_stale_multi_batch
  mixed_stale_reconnect
  mixed_terminal_producers_reconnect
  random_mixed_multi_batch
  empty_batch_reject
  empty_batch_reject_reconnect
  stale_slot_reject_reconnect
  malformed_tail_atomic
  bad_signature_tail_atomic
  raw_kunorpus_once
  raw_kunorpus_reconnect
  vote_reject_once
  vote_reject_reconnect
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
  non_atomic_single_packet
  non_atomic_inconsistent_bundle
  non_atomic_first_overdraft
  non_atomic_mid_overdraft
  non_atomic_partial_overdraft
  non_atomic_partial_overdraft_reconnect
  non_atomic_partial_resolver_fail
  non_atomic_partial_blockhash_fail
  non_atomic_partial_duplicate_sig
  non_atomic_partial_cu_fail
  non_atomic_valid_multi_packet
  bam_cu_limit_fail
  bam_cu_limit_fail_reconnect
  bam_fee_priority_commit
  bam_fee_priority_replay_after_reconnect
  bam_fee_queue_burst_reconnect
  bam_fee_source_mix_queue_burst_reconnect
  bam_fee_url_churn_priority_commit
  bam_fee_config_refresh_priority_commit
  bam_fee_config_commission_refresh_priority_commit
  bam_fee_config_refresh_queue_burst
  bam_fee_config_refresh_source_mix_queue_burst
  bam_fee_config_midqueue_source_mix_multi_reconnect
  fee_only_commit
  fee_only_reconnect
  durable_nonce_commit
  durable_nonce_reconnect
  durable_nonce_replay_after_reconnect
  durable_nonce_wrong_authority
  durable_nonce_wrong_authority_reconnect
  valid_alt_commit
  invalid_alt_missing_table
  invalid_alt_missing_table_reconnect
  stale_slot_reject
)

usage() {
  cat <<EOF2
Usage: $(basename "$0") [OPTIONS]

Options:
  --source-log-dir PATH       Existing fddev fuzzer artifact root to replay
  --log-dir PATH              Differential artifact root (default: mktemp)
  --target-pair A:B           Targets to compare. Supported: fddev:jito-agave,
                              fullfd:jito-agave (default: fddev:jito-agave)
  --iterations N              Source fddev iterations when --source-log-dir is absent
  --seed N                    Source fddev seed when --source-log-dir is absent
  --mode NAME                 Source fddev mode (default: commit_once)
  --mode-list CSV             Pass a deterministic comma-separated mode list
                              through to the source fddev generator
  --scenario-file PATH        External scripted BAM scenario to replay through
                              the source fddev generator
  --input-family NAME         Source fddev input family (default: synthetic)
  --kunorpus-system-kind NAME Source fddev System-program family selector
  --timeout-secs N            Per-target timeout (default: 180)
  --jito-solana-dir PATH      jito-solana checkout (default: ~/jito-solana)
  --profile NAME              Jito Cargo profile (default: release)
  --build-jito                Build the Jito solana-test-validator target first
  --fddev-config PATH         fddev config for source runs
  --fddev-rpc-url URL         fddev RPC URL for source runs
  --fullfd-config PATH        full Firedancer config for fullfd target runs
  --fullfd-bin PATH           full Firedancer dev binary
  --fullfd-snapshot-file PATH Use a local full snapshot for the Full Firedancer target
  --fullfd-snapshot-genesis-file PATH
                              Genesis file matched to the local full snapshot
  --fullfd-snapshot-shred-version N
                              Shred version for the snapshot's cluster
  --live-coverage-dir PATH    Write LLVM profiles from the live differential target
  --rpc-port PORT             RPC port for source fddev and, unless
                              --jito-rpc-port is set, Jito
  --jito-rpc-port PORT        Jito RPC port when different from fddev
  --bam-bind HOST:PORT        BAM test-server bind address
  --bam-url URL               BAM URL configured into both clients
  --bam-bad-url URL           Temporary bad BAM URL for URL churn scenarios
  --bam-tpu-port PORT         TPU port advertised by the BAM test server
  --bam-tpu-fwd-port PORT     TPU forward port advertised by the BAM test server
  --bam-shred-port PORT       Shred port advertised by the BAM test server
  --check-bam-shred           Capture the advertised BAM shred UDP port on the
                              source validator and require packet delivery
  --queue-burst-batch-count N Override queue-burst helper batch count
  --queue-burst-max-schedule-slot VALUE
                              Override queue-burst max_schedule_slot
  --queue-burst-close-after-results N
                              Override queue-burst reconnect point
  --faucet-port PORT          Jito faucet port
  --isolated-port-base N      Render an fddev config under --log-dir using
                              ports derived from N and pass matching BAM/RPC
                              ports through both clients
  --xfail-known               Classify configured known divergences as xfailed
  --known-failures PATH       Known-divergence TSV used with --xfail-known
  --ignore-cus-consumed       Ignore documented M-41 cus_consumed result diffs
                              while still comparing status, state, signatures,
                              loaded account size, and result ordering
  --ignore-batch-result-order Ignore arrival-order differences between otherwise
                              matching BAM batch result sets
  --ignore-loaded-accounts-data-size
                              Ignore loaded_accounts_data_size in per-tx BAM
                              results while still comparing execution success
                              and fee-payer deltas
  --ignore-chain-evidence     Ignore RPC block/signature landing evidence checks
                              for targets without a comparable chain oracle
  --base-sweep                Generate or require the current deterministic
                              non-operator base sweep.  When generating, this
                              sets --seed 0, --iterations to the sweep size,
                              --mode random, and --input-family random.  Pair
                              with --xfail-known to allow configured divergences.
  --control-plane-sweep       Generate or require the deterministic
                              Jito-comparable control-plane sweep.  When
                              generating, this sets --seed 14 --iterations 2
                              --mode random --input-family synthetic and
                              enables operator perturbations.
  --pre-fullfd-hardening-sweep
                              Generate or require the deterministic
                              Frankendancer-vs-Jito sweep used before pivoting
                              to full Firedancer.  This combines replay/seq-id,
                              queue/backpressure, control-plane, source-mix,
                              malformed-batch, and execution-semantics modes.
  --export-scenario-corpus PATH
                              Export replayable source scenarios and packet
                              files into PATH with a manifest.json
  --allow-operator-perturbations
                              Pass low-frequency operator churn into generated fddev runs
  -h, --help                  Show this help
EOF2
}

die() {
  echo "error: $*" >&2
  exit 1
}

validate_tcp_port() {
  local label="$1"
  local port="$2"
  [[ "${port}" =~ ^[0-9]+$ ]] || die "${label} must be numeric, got ${port}"
  (( port >= 1 && port <= 65535 )) || die "${label} must be in 1..65535, got ${port}"
}

summary_get() {
  local summary="$1"
  local key="$2"
  awk -F= -v key="${key}" '$1 == key { print substr($0, length(key) + 2); exit }' "${summary}"
}

comparison_status_get() {
  local comparison="$1"
  python3 - "${comparison}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    print(json.load(f).get("status", ""))
PY
}

known_comparison_matches() {
  local comparison="$1"
  local expected_statuses="$2"
  local expected_failure_classes="$3"

  [[ -f "${comparison}" ]] || return 1

  python3 - "${comparison}" "${expected_statuses:-*}" "${expected_failure_classes:-*}" <<'PY'
import json
import sys

comparison, expected_statuses, expected_failure_classes = sys.argv[1:4]
with open(comparison, "r", encoding="utf-8") as f:
    data = json.load(f)

def split_csv(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}

allowed_statuses = split_csv(expected_statuses)
allowed_classes = split_csv(expected_failure_classes)
status = str(data.get("status", ""))
classes = {str(item) for item in data.get("failure_classes", [])}

status_ok = not allowed_statuses or "*" in allowed_statuses or status in allowed_statuses
if not allowed_classes or "*" in allowed_classes:
    classes_ok = True
else:
    classes_ok = bool(classes) and classes.issubset(allowed_classes)

sys.exit(0 if status_ok and classes_ok else 1)
PY
}

is_replayable_iteration() {
  local summary="$1"
  local mode system_kind
  mode=$(summary_get "${summary}" mode)
  system_kind=$(summary_get "${summary}" system_kind)

  case "${mode}:${system_kind}" in
    commit_once:transfer|commit_once:assign|commit_once:allocate|\
    commit_once:create_account|commit_once:transfer_with_seed|\
    commit_once:create_account_with_seed|\
    non_atomic_valid_multi_packet:transfer|\
    non_atomic_valid_multi_packet:assign|\
    non_atomic_valid_multi_packet:allocate|\
    non_atomic_valid_multi_packet:create_account|\
    non_atomic_valid_multi_packet:transfer_with_seed|\
    non_atomic_valid_multi_packet:create_account_with_seed|\
    non_atomic_single_packet:transfer|\
    non_atomic_single_packet:assign|\
    non_atomic_single_packet:allocate|\
    non_atomic_single_packet:create_account|\
    non_atomic_single_packet:transfer_with_seed|\
    non_atomic_single_packet:create_account_with_seed|\
    non_atomic_first_overdraft:transfer|\
    atomic_first_overdraft:transfer|\
    atomic_first_overdraft_reconnect:transfer|\
    non_atomic_mid_overdraft:transfer|\
    non_atomic_partial_overdraft:transfer|\
    non_atomic_partial_overdraft_reconnect:transfer|\
    non_atomic_partial_resolver_fail:transfer|\
    non_atomic_partial_blockhash_fail:transfer|\
    non_atomic_partial_duplicate_sig:transfer|\
    non_atomic_partial_cu_fail:transfer|\
    stale_slot_reject_reconnect:transfer|\
    stale_slot_reject_reconnect:assign|\
    stale_slot_reject_reconnect:allocate|\
    stale_slot_reject_reconnect:create_account|\
    stale_slot_reject_reconnect:transfer_with_seed|\
    stale_slot_reject_reconnect:create_account_with_seed|\
    empty_batch_reject_reconnect:transfer|\
    empty_batch_reject_reconnect:assign|\
    empty_batch_reject_reconnect:allocate|\
    empty_batch_reject_reconnect:create_account|\
    empty_batch_reject_reconnect:transfer_with_seed|\
    empty_batch_reject_reconnect:create_account_with_seed|\
    valid_alt_commit:transfer|\
    invalid_alt_missing_table:transfer|\
    invalid_alt_missing_table_reconnect:transfer|\
    durable_nonce_wrong_authority:durable_nonce|\
    durable_nonce_wrong_authority_reconnect:durable_nonce|\
    bam_fee_priority_commit:transfer|\
    bam_fee_priority_replay_after_reconnect:transfer|\
    bam_fee_queue_burst_reconnect:|\
    bam_fee_queue_burst_reconnect:transfer|\
    bam_fee_source_mix_queue_burst_reconnect:|\
    bam_fee_source_mix_queue_burst_reconnect:transfer|\
    source_mix_queue_burst_reconnect:|\
    source_mix_queue_burst_reconnect:transfer|\
    source_mix_queue_burst_multi_reconnect:|\
    source_mix_queue_burst_multi_reconnect:transfer|\
    disable_enable_queue_burst_reconnect:|\
    disable_enable_queue_burst_reconnect:transfer|\
    stale_slot_reject_reconnect:|\
    stale_slot_reject_reconnect:transfer|\
    empty_batch_reject_reconnect:|\
    empty_batch_reject_reconnect:transfer|\
    queue_burst_multi_reconnect:|\
    queue_burst_multi_reconnect:transfer|\
    bam_fee_url_churn_priority_commit:transfer|\
    bam_fee_url_churn_same_slot_priority_commit:transfer|\
    bam_fee_config_refresh_priority_commit:transfer|\
    bam_fee_config_commission_refresh_priority_commit:transfer|\
    bam_fee_config_refresh_queue_burst:transfer|\
    bam_fee_config_refresh_source_mix_queue_burst:transfer|\
    bam_fee_config_midqueue_refresh:transfer|\
    bam_fee_config_midqueue_source_mix_queue_burst:transfer|\
    bam_fee_config_midqueue_source_mix_multi_reconnect:transfer|\
    bam_cu_limit_fail:transfer|\
    bam_cu_limit_fail_reconnect:transfer|\
    atomic_mid_fail:transfer|\
    atomic_mid_fail_reconnect:transfer|\
    atomic_blockhash_mid_fail:transfer|\
    atomic_blockhash_mid_fail_reconnect:transfer|\
    atomic_resolver_mid_fail:transfer|\
    atomic_resolver_mid_fail_reconnect:transfer|\
    atomic_duplicate_sig_mid_fail:transfer|\
    atomic_duplicate_sig_mid_fail_reconnect:transfer|\
    malformed_first_atomic:transfer|malformed_first_atomic:assign|\
    malformed_first_atomic:allocate|\
    malformed_first_atomic:create_account|\
    malformed_first_atomic:transfer_with_seed|\
    malformed_first_atomic:create_account_with_seed|\
    malformed_first_atomic_reconnect:transfer|\
    malformed_first_atomic_reconnect:assign|\
    malformed_first_atomic_reconnect:allocate|\
    malformed_first_atomic_reconnect:create_account|\
    malformed_first_atomic_reconnect:transfer_with_seed|\
    malformed_first_atomic_reconnect:create_account_with_seed|\
    malformed_tail_atomic:transfer|malformed_tail_atomic:assign|\
    malformed_tail_atomic:allocate|\
    malformed_tail_atomic:create_account|\
    malformed_tail_atomic:transfer_with_seed|\
    malformed_tail_atomic:create_account_with_seed|\
    malformed_tail_atomic_reconnect:transfer|\
    malformed_tail_atomic_reconnect:assign|\
    malformed_tail_atomic_reconnect:allocate|\
    malformed_tail_atomic_reconnect:create_account|\
    malformed_tail_atomic_reconnect:transfer_with_seed|\
    malformed_tail_atomic_reconnect:create_account_with_seed|\
    bad_signature_first_atomic:transfer|bad_signature_first_atomic:assign|\
    bad_signature_first_atomic:allocate|\
    bad_signature_first_atomic:create_account|\
    bad_signature_first_atomic:transfer_with_seed|\
    bad_signature_first_atomic:create_account_with_seed|\
    bad_signature_first_atomic_reconnect:transfer|\
    bad_signature_first_atomic_reconnect:assign|\
    bad_signature_first_atomic_reconnect:allocate|\
    bad_signature_first_atomic_reconnect:create_account|\
    bad_signature_first_atomic_reconnect:transfer_with_seed|\
    bad_signature_first_atomic_reconnect:create_account_with_seed|\
    bad_signature_tail_atomic:transfer|bad_signature_tail_atomic:assign|\
    bad_signature_tail_atomic:allocate|\
    bad_signature_tail_atomic:create_account|\
    bad_signature_tail_atomic:transfer_with_seed|\
    bad_signature_tail_atomic:create_account_with_seed|\
    bad_signature_tail_atomic_reconnect:transfer|\
    bad_signature_tail_atomic_reconnect:assign|\
    bad_signature_tail_atomic_reconnect:allocate|\
    bad_signature_tail_atomic_reconnect:create_account|\
    bad_signature_tail_atomic_reconnect:transfer_with_seed|\
    bad_signature_tail_atomic_reconnect:create_account_with_seed|\
    replay_same_conn:transfer|replay_same_conn:assign|\
    replay_same_conn:allocate|replay_same_conn:create_account|\
    replay_same_conn:transfer_with_seed|\
    replay_same_conn:create_account_with_seed|\
    replay_after_reconnect:transfer|replay_after_reconnect:assign|\
    replay_after_reconnect:allocate|\
    replay_after_reconnect:create_account|\
    replay_after_reconnect:transfer_with_seed|\
    replay_after_reconnect:create_account_with_seed|\
    seq_collision_same_conn:transfer|\
    seq_collision_same_conn:assign|\
    seq_collision_same_conn:allocate|\
    seq_collision_same_conn:create_account|\
    seq_collision_same_conn:transfer_with_seed|\
    seq_collision_same_conn:create_account_with_seed|\
    seq_collision_reconnect:transfer|\
    seq_collision_reconnect:assign|\
    seq_collision_reconnect:allocate|\
    seq_collision_reconnect:create_account|\
    seq_collision_reconnect:transfer_with_seed|\
    seq_collision_reconnect:create_account_with_seed|\
    unique_after_reconnect:transfer|unique_after_reconnect:assign|\
    unique_after_reconnect:allocate|\
    unique_after_reconnect:create_account|\
    unique_after_reconnect:transfer_with_seed|\
    unique_after_reconnect:create_account_with_seed|\
    seq_id_wrap_sequence:transfer|seq_id_wrap_sequence:assign|\
    seq_id_wrap_sequence:allocate|\
    seq_id_wrap_sequence:create_account|\
    seq_id_wrap_sequence:transfer_with_seed|\
    seq_id_wrap_sequence:create_account_with_seed|\
    seq_id_out_of_order_multi_batch:transfer|\
    seq_id_out_of_order_multi_batch:assign|\
    seq_id_out_of_order_multi_batch:allocate|\
    seq_id_out_of_order_multi_batch:create_account|\
    seq_id_out_of_order_multi_batch:transfer_with_seed|\
    seq_id_out_of_order_multi_batch:create_account_with_seed|\
    seq_id_wrap_out_of_order_multi_batch:transfer|\
    seq_id_wrap_out_of_order_multi_batch:assign|\
    seq_id_wrap_out_of_order_multi_batch:allocate|\
    seq_id_wrap_out_of_order_multi_batch:create_account|\
    seq_id_wrap_out_of_order_multi_batch:transfer_with_seed|\
    seq_id_wrap_out_of_order_multi_batch:create_account_with_seed|\
    seq_id_wrap_conflicting_spend_multi_batch:transfer|\
    disable_enable_unique_after_reconnect:transfer|\
    disable_enable_unique_after_reconnect:assign|\
    disable_enable_unique_after_reconnect:allocate|\
    disable_enable_unique_after_reconnect:create_account|\
    disable_enable_unique_after_reconnect:transfer_with_seed|\
    disable_enable_unique_after_reconnect:create_account_with_seed|\
    url_churn_unique_after_reconnect:transfer|\
    url_churn_unique_after_reconnect:assign|\
    url_churn_unique_after_reconnect:allocate|\
    url_churn_unique_after_reconnect:create_account|\
    url_churn_unique_after_reconnect:transfer_with_seed|\
    url_churn_unique_after_reconnect:create_account_with_seed|\
    source_mix_bam_tpu:transfer|\
    source_mix_precommit:transfer|\
    source_mix_atomic_revert_precommit:transfer|\
    source_mix_duplicate_tpu_after_bam:transfer|\
    disable_enable_tpu_release:transfer|\
    source_mix_queue_burst_reconnect:|source_mix_queue_burst_reconnect:transfer|\
    source_mix_queue_burst_multi_reconnect:|source_mix_queue_burst_multi_reconnect:transfer|\
    disable_enable_queue_burst_reconnect:|disable_enable_queue_burst_reconnect:transfer|\
    bam_fee_source_mix_queue_burst_reconnect:|bam_fee_source_mix_queue_burst_reconnect:transfer|\
    bam_fee_config_refresh_queue_burst:|bam_fee_config_refresh_queue_burst:transfer|\
    bam_fee_config_refresh_source_mix_queue_burst:|bam_fee_config_refresh_source_mix_queue_burst:transfer|\
    bam_fee_config_midqueue_refresh:|bam_fee_config_midqueue_refresh:transfer|\
    bam_fee_config_midqueue_source_mix_queue_burst:|bam_fee_config_midqueue_source_mix_queue_burst:transfer|\
    bam_fee_config_midqueue_source_mix_multi_reconnect:|bam_fee_config_midqueue_source_mix_multi_reconnect:transfer|\
    partial_drain_reconnect:|partial_drain_reconnect:transfer|\
    queue_burst_reconnect:|queue_burst_reconnect:transfer|\
    queue_burst64_reconnect:|queue_burst64_reconnect:transfer|\
    queue_burst64_leader_plus1_reconnect:|queue_burst64_leader_plus1_reconnect:transfer|\
    schedule_boundary_jitter:|schedule_boundary_jitter:transfer|\
    queue_reconnect_timing_jitter:|queue_reconnect_timing_jitter:transfer|\
    queue_burst_multi_reconnect:|queue_burst_multi_reconnect:transfer|\
    queue_burst128_reconnect:|queue_burst128_reconnect:transfer|\
    queue_burst256_reconnect:|queue_burst256_reconnect:transfer|\
    queue_burst512_reconnect:|queue_burst512_reconnect:transfer|\
    queue_burst_leader_reconnect:|queue_burst_leader_reconnect:transfer|\
    queue_burst64_leader_reconnect:|queue_burst64_leader_reconnect:transfer|\
    seq_id_max_once:transfer|\
    seq_id_max_once:assign|\
    seq_id_max_once:allocate|\
    seq_id_max_once:create_account|\
    seq_id_max_once:transfer_with_seed|\
    seq_id_max_once:create_account_with_seed|\
    seq_id_max_replay_after_reconnect:transfer|\
    seq_id_max_replay_after_reconnect:assign|\
    seq_id_max_replay_after_reconnect:allocate|\
    seq_id_max_replay_after_reconnect:create_account|\
    seq_id_max_replay_after_reconnect:transfer_with_seed|\
    seq_id_max_replay_after_reconnect:create_account_with_seed|\
    stale_slot_reject:transfer|stale_slot_reject:assign|\
    stale_slot_reject:allocate|stale_slot_reject:create_account|\
    stale_slot_reject:transfer_with_seed|\
    stale_slot_reject:create_account_with_seed|\
    empty_batch_reject:transfer|empty_batch_reject:assign|\
    empty_batch_reject:allocate|empty_batch_reject:create_account|\
    empty_batch_reject:transfer_with_seed|\
    empty_batch_reject:create_account_with_seed|\
    empty_batch_reject_reconnect:transfer|\
    empty_batch_reject_reconnect:assign|\
    empty_batch_reject_reconnect:allocate|\
    empty_batch_reject_reconnect:create_account|\
    empty_batch_reject_reconnect:transfer_with_seed|\
    empty_batch_reject_reconnect:create_account_with_seed|\
    mixed_multi_batch:transfer|mixed_multi_batch:assign|\
    mixed_multi_batch:allocate|mixed_multi_batch:create_account|\
    mixed_multi_batch:transfer_with_seed|\
    mixed_multi_batch:create_account_with_seed|\
    mixed_empty_multi_batch:transfer|mixed_empty_multi_batch:assign|\
    mixed_empty_multi_batch:allocate|\
    mixed_empty_multi_batch:create_account|\
    mixed_empty_multi_batch:transfer_with_seed|\
    mixed_empty_multi_batch:create_account_with_seed|\
    mixed_malformed_multi_batch:transfer|\
    mixed_malformed_multi_batch:assign|\
    mixed_malformed_multi_batch:allocate|\
    mixed_malformed_multi_batch:create_account|\
    mixed_malformed_multi_batch:transfer_with_seed|\
    mixed_malformed_multi_batch:create_account_with_seed|\
    mixed_bad_signature_multi_batch:transfer|\
    mixed_bad_signature_multi_batch:assign|\
    mixed_bad_signature_multi_batch:allocate|\
    mixed_bad_signature_multi_batch:create_account|\
    mixed_bad_signature_multi_batch:transfer_with_seed|\
    mixed_bad_signature_multi_batch:create_account_with_seed|\
    mixed_bad_signature_reconnect:transfer|\
    mixed_bad_signature_reconnect:assign|\
    mixed_bad_signature_reconnect:allocate|\
    mixed_bad_signature_reconnect:create_account|\
    mixed_bad_signature_reconnect:transfer_with_seed|\
    mixed_bad_signature_reconnect:create_account_with_seed|\
    mixed_stale_multi_batch:transfer|mixed_stale_multi_batch:assign|\
    mixed_stale_multi_batch:allocate|\
    mixed_stale_multi_batch:create_account|\
    mixed_stale_multi_batch:transfer_with_seed|\
    mixed_stale_multi_batch:create_account_with_seed|\
    mixed_stale_reconnect:transfer|mixed_stale_reconnect:assign|\
    mixed_stale_reconnect:allocate|\
    mixed_stale_reconnect:create_account|\
    mixed_stale_reconnect:transfer_with_seed|\
    mixed_stale_reconnect:create_account_with_seed|\
    mixed_terminal_producers_reconnect:transfer|\
    mixed_terminal_producers_reconnect:assign|\
    mixed_terminal_producers_reconnect:allocate|\
    mixed_terminal_producers_reconnect:create_account|\
    mixed_terminal_producers_reconnect:transfer_with_seed|\
    mixed_terminal_producers_reconnect:create_account_with_seed|\
    random_mixed_multi_batch:transfer|random_mixed_multi_batch:assign|\
    random_mixed_multi_batch:allocate|\
    random_mixed_multi_batch:create_account|\
    random_mixed_multi_batch:transfer_with_seed|\
    random_mixed_multi_batch:create_account_with_seed|\
    external_scenario:*|\
    stale_slot_reject_reconnect:|stale_slot_reject_reconnect:transfer|\
    empty_batch_reject_reconnect:|empty_batch_reject_reconnect:transfer|\
    atomic_revert:|atomic_revert:transfer|\
    atomic_revert_reconnect:|atomic_revert_reconnect:transfer|\
    atomic_first_overdraft:|atomic_first_overdraft:transfer|\
    atomic_first_overdraft_reconnect:|atomic_first_overdraft_reconnect:transfer|\
    atomic_mid_fail_reconnect:|atomic_mid_fail_reconnect:transfer|\
    atomic_blockhash_mid_fail_reconnect:|atomic_blockhash_mid_fail_reconnect:transfer|\
    atomic_resolver_mid_fail_reconnect:|atomic_resolver_mid_fail_reconnect:transfer|\
    atomic_duplicate_sig_mid_fail:|atomic_duplicate_sig_mid_fail:transfer|\
    atomic_duplicate_sig_mid_fail_reconnect:|atomic_duplicate_sig_mid_fail_reconnect:transfer|\
    malformed_first_atomic:|malformed_first_atomic:transfer|\
    malformed_first_atomic_reconnect:|malformed_first_atomic_reconnect:transfer|\
    malformed_tail_atomic_reconnect:|malformed_tail_atomic_reconnect:transfer|\
    bad_signature_first_atomic:|bad_signature_first_atomic:transfer|\
    bad_signature_first_atomic_reconnect:|bad_signature_first_atomic_reconnect:transfer|\
    bad_signature_tail_atomic_reconnect:|bad_signature_tail_atomic_reconnect:transfer|\
    source_mix_atomic_revert_precommit:|source_mix_atomic_revert_precommit:transfer|\
    disable_enable_tpu_release:|disable_enable_tpu_release:transfer|\
    fee_only_commit:fee_only|\
    fee_only_reconnect:fee_only|\
    durable_nonce_commit:durable_nonce|\
    durable_nonce_reconnect:durable_nonce|\
    durable_nonce_replay_after_reconnect:durable_nonce|\
    durable_nonce_wrong_authority:durable_nonce|\
    durable_nonce_wrong_authority_reconnect:durable_nonce|\
    invalid_alt_missing_table:transfer|\
    invalid_alt_missing_table_reconnect:transfer|\
    duplicate_seq_split:|duplicate_seq_split:transfer|\
    duplicate_seq_split_reconnect:|duplicate_seq_split_reconnect:transfer|\
    non_atomic_inconsistent_bundle:|\
    vote_reject_once:simple_vote|\
    vote_reject_reconnect:simple_vote|\
    raw_kunorpus_once:raw_kunorpus_txn|\
    raw_kunorpus_reconnect:raw_kunorpus_txn)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

known_failure_id() {
  local mode="$1"
  local input_family="$2"
  local system_kind="$3"
  local comparison="$4"

  [[ "${XFAIL_KNOWN}" == "1" ]] || return 1
  [[ -f "${KNOWN_FAILURES_FILE}" ]] || die "missing known failures file ${KNOWN_FAILURES_FILE}"

  local known_mode known_input known_system finding_id expected_statuses expected_failure_classes _note
  while IFS=$'\t' read -r known_mode known_input known_system finding_id expected_statuses expected_failure_classes _note || [[ -n "${known_mode:-}" ]]; do
    [[ -n "${known_mode:-}" ]] || continue
    [[ "${known_mode}" == \#* ]] && continue
    [[ "${known_mode}" == "*" || "${known_mode}" == "${mode}" ]] || continue
    [[ "${known_input}" == "*" || "${known_input}" == "${input_family}" ]] || continue
    [[ "${known_system}" == "*" || "${known_system}" == "${system_kind}" ]] || continue
    [[ -n "${finding_id:-}" ]] || finding_id="known"
    expected_statuses="${expected_statuses:-*}"
    expected_failure_classes="${expected_failure_classes:-*}"
    if known_comparison_matches "${comparison}" "${expected_statuses}" "${expected_failure_classes}"; then
      printf '%s\n' "${finding_id}"
      return 0
    fi
  done <"${KNOWN_FAILURES_FILE}"
  return 1
}

join_csv() {
  local out="" item
  for item in "$@"; do
    if [[ -z "${out}" ]]; then
      out="${item}"
    else
      out="${out},${item}"
    fi
  done
  printf '%s\n' "${out}"
}

write_external_source_iteration() {
  local source_root="$1"
  local scenario_file="$2"
  local seed="$3"
  local iter_dir="${source_root}/iter_001_external_scenario_external_scenario"

  rm -rf -- "${source_root}"
  mkdir -p "${iter_dir}"
  cp -- "${scenario_file}" "${iter_dir}/scenario.toml"

  cat >"${iter_dir}/summary.txt" <<EOF2
seed=${seed}
mode=external_scenario
input_family=external_scenario
start_slot=0
end_slot=0
scenario_file=${iter_dir}/scenario.toml
requested_kunorpus_system_kind=${KUNORPUS_SYSTEM_KIND}
system_kind=transfer
input_path=${iter_dir}/scenario.toml
input_label=external_scripted_scenario
input_note=external_scenario_file=${iter_dir}/scenario.toml synthetic_source=1
seq_one=1000
seq_two=1001
seq_three=1002
payer=n/a
recipient_one=n/a
recipient_two=n/a
EOF2
}

write_seed_source_iterations() {
  local source_root="$1"
  shift
  local modes=("$@")

  rm -rf -- "${source_root}"
  mkdir -p "${source_root}"

  local idx=1 mode iter_seed iter_dir input_family_name
  if (( ${#modes[@]} == 0 )); then
    modes=("${MODE}")
  fi
  for mode in "${modes[@]}"; do
    iter_seed=$((SEED + idx - 1))
    input_family_name="${INPUT_FAMILY}"
    if [[ "${mode}" == "external_scenario" ]]; then
      input_family_name="external_scenario"
    fi
    iter_dir=$(printf '%s/iter_%03d_%s_%s' "${source_root}" "${idx}" "${mode}" "${input_family_name}")
    mkdir -p "${iter_dir}"
    cat >"${iter_dir}/scenario.toml" <<EOF2
# seed-only source stub; the fullfd adapter generates the target-local scenario.
EOF2
    cat >"${iter_dir}/summary.txt" <<EOF2
seed=${iter_seed}
mode=${mode}
input_family=${input_family_name}
start_slot=0
end_slot=0
scenario_file=${iter_dir}/scenario.toml
requested_kunorpus_system_kind=${KUNORPUS_SYSTEM_KIND}
system_kind=
input_path=
input_label=fullfd_seed_source
input_note=fullfd_seed_source=1
seq_one=
seq_two=
seq_three=
payer=n/a
recipient_one=n/a
recipient_two=n/a
EOF2
    ((idx += 1))
  done
}

split_csv_modes() {
  local csv="$1"
  local -n out_ref="$2"
  local IFS=,
  read -r -a out_ref <<<"${csv}"
}

missing_modes() {
  local -n covered_ref="$1"
  local -n required_ref="$2"
  local -a missing=()
  local required_mode
  for required_mode in "${required_ref[@]}"; do
    if [[ "${covered_ref[${required_mode}]:-0}" != "1" ]]; then
      missing+=("${required_mode}")
    fi
  done
  join_csv "${missing[@]}"
}

while (($#)); do
  case "$1" in
    --source-log-dir)
      SOURCE_LOG_DIR="$2"
      shift 2
      ;;
    --log-dir)
      LOG_DIR="$2"
      shift 2
      ;;
    --target-pair)
      TARGET_PAIR="$2"
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
      MODE_EXPLICIT=1
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
    --input-family)
      INPUT_FAMILY="$2"
      shift 2
      ;;
    --kunorpus-system-kind)
      KUNORPUS_SYSTEM_KIND="$2"
      shift 2
      ;;
    --timeout-secs)
      TIMEOUT_SECS="$2"
      shift 2
      ;;
    --jito-solana-dir)
      JITO_SOLANA_DIR="$2"
      shift 2
      ;;
    --profile)
      JITO_PROFILE="$2"
      shift 2
      ;;
    --build-jito)
      BUILD_JITO=1
      shift
      ;;
    --fddev-config)
      FDDEV_CONFIG="$2"
      shift 2
      ;;
    --fddev-rpc-url)
      FDDEV_RPC_URL="$2"
      shift 2
      ;;
    --fullfd-config)
      FULLFD_CONFIG="$2"
      shift 2
      ;;
    --fullfd-bin)
      FULLFD_BIN="$2"
      shift 2
      ;;
    --fullfd-snapshot-file)
      FULLFD_SNAPSHOT_FILE="$2"
      shift 2
      ;;
    --fullfd-snapshot-genesis-file)
      FULLFD_SNAPSHOT_GENESIS_FILE="$2"
      shift 2
      ;;
    --fullfd-snapshot-shred-version)
      FULLFD_SNAPSHOT_SHRED_VERSION="$2"
      shift 2
      ;;
    --live-coverage-dir)
      LIVE_COVERAGE_DIR="$2"
      shift 2
      ;;
    --rpc-port)
      RPC_PORT="$2"
      shift 2
      ;;
    --jito-rpc-port)
      JITO_RPC_PORT="$2"
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
    --faucet-port)
      FAUCET_PORT="$2"
      FAUCET_PORT_EXPLICIT=1
      shift 2
      ;;
    --isolated-port-base)
      ISOLATED_PORT_BASE="$2"
      shift 2
      ;;
    --xfail-known)
      XFAIL_KNOWN=1
      shift
      ;;
    --known-failures)
      KNOWN_FAILURES_FILE="$2"
      shift 2
      ;;
    --ignore-cus-consumed)
      IGNORE_CUS_CONSUMED=1
      shift
      ;;
    --ignore-batch-result-order)
      IGNORE_BATCH_RESULT_ORDER=1
      shift
      ;;
    --ignore-loaded-accounts-data-size)
      IGNORE_LOADED_ACCOUNTS_DATA_SIZE=1
      shift
      ;;
    --ignore-chain-evidence)
      IGNORE_CHAIN_EVIDENCE=1
      shift
      ;;
    --base-sweep)
      BASE_SWEEP=1
      shift
      ;;
    --control-plane-sweep)
      CONTROL_PLANE_SWEEP=1
      shift
      ;;
    --pre-fullfd-hardening-sweep)
      PRE_FULLFD_HARDENING_SWEEP=1
      shift
      ;;
    --allow-operator-perturbations)
      ALLOW_OPERATOR_PERTURBATIONS=1
      shift
      ;;
    --export-scenario-corpus)
      EXPORT_SCENARIO_CORPUS="$2"
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

[[ -x "${ROOT}/contrib/fuzz-local-bam-stateful.sh" ]] || die "missing fuzz-local-bam-stateful.sh"
[[ -x "${ROOT}/contrib/run-jito-agave-bam-scenario.sh" ]] || die "missing run-jito-agave-bam-scenario.sh"
[[ -x "${ROOT}/contrib/run-fullfd-bam-scenario.sh" ]] || die "missing run-fullfd-bam-scenario.sh"
[[ -x "${ROOT}/contrib/compare-bam-outcomes.py" ]] || die "missing compare-bam-outcomes.py"
[[ -x "${ROOT}/contrib/export-bam-diff-corpus.py" ]] || die "missing export-bam-diff-corpus.py"
[[ -x "${ROOT}/contrib/render-bam-local-config.py" ]] || die "missing render-bam-local-config.py"

case "${TARGET_PAIR}" in
  fddev:jito-agave|fullfd:jito-agave)
    ;;
  *)
    die "unsupported --target-pair ${TARGET_PAIR}; expected fddev:jito-agave or fullfd:jito-agave"
    ;;
esac

if [[ -n "${FULLFD_SNAPSHOT_FILE}" ]]; then
  [[ "${TARGET_PAIR}" == "fullfd:jito-agave" ]] \
    || die "--fullfd-snapshot-file requires --target-pair fullfd:jito-agave"
  [[ -r "${FULLFD_SNAPSHOT_FILE}" ]] \
    || die "missing full Firedancer snapshot ${FULLFD_SNAPSHOT_FILE}"
  [[ -r "${FULLFD_SNAPSHOT_GENESIS_FILE}" ]] \
    || die "missing snapshot-matched genesis ${FULLFD_SNAPSHOT_GENESIS_FILE}"
  [[ "${FULLFD_SNAPSHOT_SHRED_VERSION}" =~ ^[0-9]+$ ]] \
    && (( FULLFD_SNAPSHOT_SHRED_VERSION >= 1 && FULLFD_SNAPSHOT_SHRED_VERSION <= 65535 )) \
    || die "--fullfd-snapshot-shred-version must be an integer from 1 through 65535"
  [[ -n "${ISOLATED_PORT_BASE}" ]] \
    || die "--fullfd-snapshot-file requires --isolated-port-base"
fi

if [[ "${TARGET_PAIR}" == "fullfd:jito-agave" ]]; then
  IGNORE_LOADED_ACCOUNTS_DATA_SIZE=1
  IGNORE_CHAIN_EVIDENCE=1
fi

if (( BASE_SWEEP + CONTROL_PLANE_SWEEP + PRE_FULLFD_HARDENING_SWEEP > 1 )); then
  die "--base-sweep, --control-plane-sweep, and --pre-fullfd-hardening-sweep are mutually exclusive"
fi

if [[ "${BASE_SWEEP}" == "1" && -z "${SOURCE_LOG_DIR}" ]]; then
  SEED=0
  ITERATIONS=${#BASE_SWEEP_MODES[@]}
  MODE="random"
  INPUT_FAMILY="random"
  KUNORPUS_SYSTEM_KIND="any"
fi
if [[ "${CONTROL_PLANE_SWEEP}" == "1" && -z "${SOURCE_LOG_DIR}" ]]; then
  SEED=14
  ITERATIONS=${#CONTROL_PLANE_SWEEP_MODES[@]}
  MODE="random"
  INPUT_FAMILY="synthetic"
  KUNORPUS_SYSTEM_KIND="any"
  ALLOW_OPERATOR_PERTURBATIONS=1
fi
if [[ "${PRE_FULLFD_HARDENING_SWEEP}" == "1" && -z "${SOURCE_LOG_DIR}" ]]; then
  SEED=42
  ITERATIONS=${#PRE_FULLFD_HARDENING_SWEEP_MODES[@]}
  MODE="random"
  INPUT_FAMILY="random"
  KUNORPUS_SYSTEM_KIND="any"
  ALLOW_OPERATOR_PERTURBATIONS=1
fi
if [[ -n "${EXTERNAL_SCENARIO_FILE}" && "${MODE_EXPLICIT}" == "0" && "${BASE_SWEEP}" != "1" && "${CONTROL_PLANE_SWEEP}" != "1" && "${PRE_FULLFD_HARDENING_SWEEP}" != "1" ]]; then
  MODE="external_scenario"
fi

if [[ -z "${LOG_DIR}" ]]; then
  LOG_DIR=$(mktemp -d /tmp/firebam-diff-jito.XXXXXX)
fi
mkdir -p "${LOG_DIR}"

render_config_extra_args=()
fullfd_render_config_extra_args=()
fullfd_snapshot_render_args=()
fullfd_runner_env=()
fullfd_render_has_paths_genesis=0
jito_extra_args=()
jito_runner_env=()
if [[ "${TARGET_PAIR}" == "fullfd:jito-agave" ]]; then
  fullfd_render_config_extra_args+=(--paths-base "${LOG_DIR}/fullfd-state")
fi
if [[ -n "${GUI_PORT}" ]]; then
  validate_tcp_port "GUI port" "${GUI_PORT}"
  render_config_extra_args+=(--gui-port "${GUI_PORT}")
  if [[ "${TARGET_PAIR}" == "fddev:jito-agave" ]]; then
    render_config_extra_args+=(--layout-affinity "1-16" --agave-affinity "17-32")
  fi
  GUI_URL="${GUI_URL:-http://127.0.0.1:${GUI_PORT}}"
  export GUI_URL
fi
if mode_selection_contains bam_fee_url_churn_same_slot_priority_commit; then
  [[ "${LONG_SLOT_TICKS_PER_SLOT}" =~ ^[0-9]+$ && "${LONG_SLOT_TICKS_PER_SLOT}" -gt 0 ]] \
    || die "LONG_SLOT_TICKS_PER_SLOT must be a positive integer"
  [[ "${LONG_SLOT_TARGET_TICK_DURATION_MICROS}" =~ ^[0-9]+$ && "${LONG_SLOT_TARGET_TICK_DURATION_MICROS}" -gt 0 ]] \
    || die "LONG_SLOT_TARGET_TICK_DURATION_MICROS must be a positive integer"
  [[ "${LONG_SLOT_HASHES_PER_TICK}" =~ ^[0-9]+$ && "${LONG_SLOT_HASHES_PER_TICK}" -gt 0 ]] \
    || die "LONG_SLOT_HASHES_PER_TICK must be a positive integer"
  render_config_extra_args+=(
    --genesis-hashes-per-tick "${LONG_SLOT_HASHES_PER_TICK}"
    --genesis-target-tick-duration-micros "${LONG_SLOT_TARGET_TICK_DURATION_MICROS}"
    --genesis-ticks-per-slot "${LONG_SLOT_TICKS_PER_SLOT}"
  )
  fullfd_render_config_extra_args+=(--paths-genesis "${LOG_DIR}/fullfd-state/genesis.bin")
  fullfd_render_has_paths_genesis=1
  JITO_TICKS_PER_SLOT="${LONG_SLOT_TICKS_PER_SLOT}"
  jito_extra_args+=(--ticks-per-slot "${JITO_TICKS_PER_SLOT}")
fi
if [[ "${TARGET_PAIR}" == "fullfd:jito-agave" && "${fullfd_render_has_paths_genesis}" == "0" ]] && mode_selection_contains valid_alt_commit; then
  fullfd_render_config_extra_args+=(--paths-genesis "${LOG_DIR}/fullfd-state/genesis.bin")
  fullfd_render_has_paths_genesis=1
fi

if [[ -n "${ISOLATED_PORT_BASE}" ]]; then
  [[ "${ISOLATED_PORT_BASE}" =~ ^[0-9]+$ ]] || die "--isolated-port-base must be numeric"
  regular_tpu_port=$((ISOLATED_PORT_BASE + 1))
  quic_tpu_port=$((ISOLATED_PORT_BASE + 7))
  fddev_shred_listen_port=$((ISOLATED_PORT_BASE + 3))
  gossip_port=$((ISOLATED_PORT_BASE + 40))
  if [[ -n "${FULLFD_SNAPSHOT_FILE}" ]]; then
    fullfd_snapshot_render_args=(
      --local-snapshot-mode
      --gossip-entrypoint "127.0.0.1:${gossip_port}"
      --expected-shred-version "${FULLFD_SNAPSHOT_SHRED_VERSION}"
    )
  fi
  RPC_PORT=$((ISOLATED_PORT_BASE + 99))
  if [[ -z "${JITO_RPC_PORT}" ]]; then
    JITO_RPC_PORT=$((ISOLATED_PORT_BASE + 199))
  fi
  BAM_TPU_PORT="${quic_tpu_port}"
  BAM_TPU_FWD_PORT=$((ISOLATED_PORT_BASE + 8))
  BAM_SHRED_PORT=$((ISOLATED_PORT_BASE + 9))
  BAM_BIND="127.0.0.1:$((ISOLATED_PORT_BASE + 55))"
  BAM_URL="http://${BAM_BIND}"
  BUNDLE_URL="http://127.0.0.1:$((ISOLATED_PORT_BASE + 51))"
  BAM_BAD_URL="http://127.0.0.1:$((ISOLATED_PORT_BASE + 56))"
  if [[ "${FAUCET_PORT_EXPLICIT}" != "1" ]]; then
    FAUCET_PORT=$((ISOLATED_PORT_BASE + 100))
  fi
  if [[ "${FAUCET_PORT}" == "0" ]]; then
    FAUCET_PORT=$(choose_free_tcp_port)
  fi
  validate_tcp_port "--isolated-port-base + 1 regular TPU port" "${regular_tpu_port}"
  validate_tcp_port "--isolated-port-base + 7 QUIC TPU port" "${quic_tpu_port}"
  validate_tcp_port "--isolated-port-base + 3 shred listen port" "${fddev_shred_listen_port}"
  validate_tcp_port "--isolated-port-base + 40 gossip port" "${gossip_port}"
  validate_tcp_port "--isolated-port-base + 99 fddev RPC port" "${RPC_PORT}"
  validate_tcp_port "--isolated-port-base + 199 Jito RPC port" "${JITO_RPC_PORT}"
  validate_tcp_port "--isolated-port-base + 8 BAM TPU forward port" "${BAM_TPU_FWD_PORT}"
  validate_tcp_port "--isolated-port-base + 9 BAM shred port" "${BAM_SHRED_PORT}"
  validate_tcp_port "--isolated-port-base + 51 bundle URL port" "$((ISOLATED_PORT_BASE + 51))"
  validate_tcp_port "--isolated-port-base + 55 BAM bind port" "$((ISOLATED_PORT_BASE + 55))"
  validate_tcp_port "--isolated-port-base + 56 bad BAM port" "$((ISOLATED_PORT_BASE + 56))"
  validate_tcp_port "faucet port" "${FAUCET_PORT}"
  FDDEV_RPC_URL="http://127.0.0.1:${RPC_PORT}"
  FDDEV_CONFIG="${LOG_DIR}/fddev-isolated.toml"
  "${ROOT}/contrib/render-bam-local-config.py" \
    --template "${FDDEV_CONFIG_TEMPLATE}" \
    --output "${FDDEV_CONFIG}" \
    --bam-url "${BAM_URL}" \
    --bundle-url "${BUNDLE_URL}" \
    --rpc-port "${RPC_PORT}" \
    --gossip-port "${gossip_port}" \
    --regular-tpu-port "${regular_tpu_port}" \
    --quic-tpu-port "${quic_tpu_port}" \
    --shred-listen-port "${fddev_shred_listen_port}" \
    "${render_config_extra_args[@]}"
  if [[ "${TARGET_PAIR}" == "fullfd:jito-agave" ]]; then
    FULLFD_CONFIG="${LOG_DIR}/fullfd-isolated.toml"
    "${ROOT}/contrib/render-bam-local-config.py" \
      --template "${FULLFD_CONFIG_TEMPLATE}" \
      --output "${FULLFD_CONFIG}" \
      --bam-url "${BAM_URL}" \
      --bundle-url "${BUNDLE_URL}" \
      --rpc-port "${RPC_PORT}" \
      --gossip-port "${gossip_port}" \
      --regular-tpu-port "${regular_tpu_port}" \
      --quic-tpu-port "${quic_tpu_port}" \
      --shred-listen-port "${fddev_shred_listen_port}" \
      "${render_config_extra_args[@]}" \
      "${fullfd_render_config_extra_args[@]}" \
      "${fullfd_snapshot_render_args[@]}"
    if [[ -n "${FULLFD_SNAPSHOT_FILE}" ]]; then
      FULLFD_SNAPSHOT_BOOTSTRAP_CONFIG="${LOG_DIR}/fullfd-bootstrap.toml"
      FULLFD_SNAPSHOT_DIR="${LOG_DIR}/fullfd-state/snapshots"
      "${ROOT}/contrib/render-bam-local-config.py" \
        --template "${FULLFD_CONFIG_TEMPLATE}" \
        --output "${FULLFD_SNAPSHOT_BOOTSTRAP_CONFIG}" \
        --bam-url "${BAM_URL}" \
        --bundle-url "${BUNDLE_URL}" \
        --rpc-port "${RPC_PORT}" \
        --gossip-port "${gossip_port}" \
        --regular-tpu-port "${regular_tpu_port}" \
        --quic-tpu-port "${quic_tpu_port}" \
        --shred-listen-port "${fddev_shred_listen_port}" \
        "${render_config_extra_args[@]}" \
        "${fullfd_render_config_extra_args[@]}"
      fullfd_runner_env+=(
        "FULLFD_SNAPSHOT_FILE=${FULLFD_SNAPSHOT_FILE}"
        "FULLFD_SNAPSHOT_GENESIS_FILE=${FULLFD_SNAPSHOT_GENESIS_FILE}"
        "FULLFD_SNAPSHOT_GENESIS_DEST=${LOG_DIR}/fullfd-state/genesis.bin"
        "FULLFD_SNAPSHOT_DIR=${FULLFD_SNAPSHOT_DIR}"
        "FULLFD_SNAPSHOT_BOOTSTRAP_CONFIG=${FULLFD_SNAPSHOT_BOOTSTRAP_CONFIG}"
      )
    fi
  fi
fi

if [[ -z "${FDDEV_CONFIG}" ]]; then
  FDDEV_CONFIG="${FDDEV_CONFIG_TEMPLATE}"
fi
if [[ "${TARGET_PAIR}" == "fullfd:jito-agave" && -z "${FULLFD_CONFIG}" ]]; then
  FULLFD_CONFIG="${FULLFD_CONFIG_TEMPLATE}"
fi
if [[ -z "${FDDEV_RPC_URL}" ]]; then
  FDDEV_RPC_URL="http://127.0.0.1:${RPC_PORT}"
fi
if [[ -z "${JITO_RPC_PORT}" ]]; then
  JITO_RPC_PORT="${RPC_PORT}"
fi
if [[ "${FAUCET_PORT}" == "0" ]]; then
  FAUCET_PORT=$(choose_free_tcp_port)
fi
if [[ "${IGNORE_CHAIN_EVIDENCE}" == "1" ]]; then
  jito_runner_env+=(CAPTURE_RPC_EVIDENCE=0 CAPTURE_BLOCK_SLOT_LIMIT=0)
fi
if [[ "${TARGET_PAIR}" == "fullfd:jito-agave" ]]; then
  jito_runner_env+=(BALANCE_OBSERVE_TIMEOUT_SECS="${JITO_BALANCE_OBSERVE_TIMEOUT_SECS}")
fi

JITO_BUILD_PATH="${JITO_SOLANA_DIR}/target/${JITO_PROFILE}"
jito_profile_flag=()
if [[ "${JITO_PROFILE}" == "release" ]]; then
  jito_profile_flag=( --release )
fi
if [[ "${BUILD_JITO}" == "1" && "${TARGET_PAIR}" == "fullfd:jito-agave" ]]; then
  [[ -d "${JITO_SOLANA_DIR}" ]] || die "missing jito-solana checkout ${JITO_SOLANA_DIR}"
  (
    cd "${JITO_SOLANA_DIR}"
    cargo build "${jito_profile_flag[@]}" \
      --bin solana \
      --bin solana-keygen \
      --bin solana-test-validator \
      --bin agave-validator
  )
  BUILD_JITO=0
fi
if [[ -d "${JITO_BUILD_PATH}" ]]; then
  export PATH="${JITO_BUILD_PATH}:${PATH}"
fi

if [[ "${TARGET_PAIR}" == "fullfd:jito-agave" ]]; then
  [[ -x "${FULLFD_BIN}" ]] || die "missing ${FULLFD_BIN}; build firedancer-dev first"
  [[ -f "${FULLFD_CONFIG}" ]] || die "missing fullfd config ${FULLFD_CONFIG}"
fi

if [[ -z "${SOURCE_LOG_DIR}" ]]; then
  if [[ "${TARGET_PAIR}" == "fullfd:jito-agave" ]]; then
    SOURCE_LOG_DIR="${LOG_DIR}/source"
    rm -rf -- "${SOURCE_LOG_DIR}" "${LOG_DIR}/jito-agave" "${LOG_DIR}/fullfd" "${LOG_DIR}/comparisons"
    if [[ -n "${EXTERNAL_SCENARIO_FILE}" ]]; then
      write_external_source_iteration "${SOURCE_LOG_DIR}" "${EXTERNAL_SCENARIO_FILE}" "${SEED}"
    else
      fullfd_source_modes=()
      if [[ "${BASE_SWEEP}" == "1" ]]; then
        fullfd_source_modes=("${BASE_SWEEP_MODES[@]}")
      elif [[ "${CONTROL_PLANE_SWEEP}" == "1" ]]; then
        fullfd_source_modes=("${CONTROL_PLANE_SWEEP_MODES[@]}")
      elif [[ "${PRE_FULLFD_HARDENING_SWEEP}" == "1" ]]; then
        fullfd_source_modes=("${PRE_FULLFD_HARDENING_SWEEP_MODES[@]}")
      elif [[ -n "${MODE_LIST}" ]]; then
        split_csv_modes "${MODE_LIST}" fullfd_source_modes
      else
        for ((i=0; i<ITERATIONS; i++)); do
          fullfd_source_modes+=("${MODE}")
        done
      fi
      write_seed_source_iterations "${SOURCE_LOG_DIR}" "${fullfd_source_modes[@]}"
    fi
  else
  SOURCE_LOG_DIR="${LOG_DIR}/fddev"
  rm -rf -- "${SOURCE_LOG_DIR}" "${LOG_DIR}/jito-agave" "${LOG_DIR}/fullfd" "${LOG_DIR}/comparisons"
  fuzz_args=(
    --iterations "${ITERATIONS}"
    --seed "${SEED}"
    --mode "${MODE}"
    --input-family "${INPUT_FAMILY}"
    --kunorpus-system-kind "${KUNORPUS_SYSTEM_KIND}"
    --config "${FDDEV_CONFIG}"
    --rpc-url "${FDDEV_RPC_URL}"
    --bam-bind "${BAM_BIND}"
    --bam-url "${BAM_URL}"
    --bam-bad-url "${BAM_BAD_URL}"
    --bam-tpu-port "${BAM_TPU_PORT}"
    --bam-tpu-fwd-port "${BAM_TPU_FWD_PORT}"
    --bam-shred-port "${BAM_SHRED_PORT}"
    --log-dir "${SOURCE_LOG_DIR}"
  )
  if [[ "${CHECK_BAM_SHRED}" == "1" ]]; then
    fuzz_args+=( --check-bam-shred )
  fi
  if [[ "${BASE_SWEEP}" == "1" ]]; then
    fuzz_args+=( --mode-list "$(join_csv "${BASE_SWEEP_MODES[@]}")" )
  elif [[ "${CONTROL_PLANE_SWEEP}" == "1" ]]; then
    fuzz_args+=( --mode-list "$(join_csv "${CONTROL_PLANE_SWEEP_MODES[@]}")" )
  elif [[ "${PRE_FULLFD_HARDENING_SWEEP}" == "1" ]]; then
    fuzz_args+=( --mode-list "$(join_csv "${PRE_FULLFD_HARDENING_SWEEP_MODES[@]}")" )
  elif [[ -n "${MODE_LIST}" ]]; then
    fuzz_args+=( --mode-list "${MODE_LIST}" )
  fi
  if [[ -n "${EXTERNAL_SCENARIO_FILE}" ]]; then
    fuzz_args+=( --scenario-file "${EXTERNAL_SCENARIO_FILE}" )
  fi
  if [[ -n "${QUEUE_BURST_BATCH_COUNT}" ]]; then
    fuzz_args+=( --queue-burst-batch-count "${QUEUE_BURST_BATCH_COUNT}" )
  fi
  if [[ -n "${QUEUE_BURST_MAX_SCHEDULE_SLOT}" ]]; then
    fuzz_args+=( --queue-burst-max-schedule-slot "${QUEUE_BURST_MAX_SCHEDULE_SLOT}" )
  fi
  if [[ -n "${QUEUE_BURST_CLOSE_AFTER_RESULTS}" ]]; then
    fuzz_args+=( --queue-burst-close-after-results "${QUEUE_BURST_CLOSE_AFTER_RESULTS}" )
  fi
  if [[ -n "${LIVE_COVERAGE_DIR}" ]]; then
    fuzz_args+=( --live-coverage-dir "${LIVE_COVERAGE_DIR}" )
  fi
  if [[ "${ALLOW_OPERATOR_PERTURBATIONS}" == "1" ]]; then
    fuzz_args+=( --allow-operator-perturbations )
  fi
  ALLOW_SELF_CHECK_FAILURES="${XFAIL_KNOWN}" \
  "${ROOT}/contrib/fuzz-local-bam-stateful.sh" \
    "${fuzz_args[@]}"
  fi
fi

[[ -d "${SOURCE_LOG_DIR}" ]] || die "missing source log dir ${SOURCE_LOG_DIR}"

JITO_ROOT="${LOG_DIR}/jito-agave"
FULLFD_ROOT="${LOG_DIR}/fullfd"
COMPARISON_ROOT="${LOG_DIR}/comparisons"
mkdir -p "${JITO_ROOT}" "${FULLFD_ROOT}" "${COMPARISON_ROOT}"

SUMMARY_TSV="${LOG_DIR}/summary.tsv"
printf 'status\titeration\tmode\tinput_family\tsystem_kind\tartifact\n' >"${SUMMARY_TSV}"

build_arg=()
if [[ "${BUILD_JITO}" == "1" ]]; then
  build_arg=( --build )
fi
fullfd_solana_args=()
if [[ -d "${JITO_BUILD_PATH}" ]]; then
  fullfd_solana_args=( --solana-bin-dir "${JITO_BUILD_PATH}" )
fi
fullfd_check_bam_shred_args=()
if [[ "${CHECK_BAM_SHRED}" == "1" ]]; then
  fullfd_check_bam_shred_args=( --check-bam-shred )
fi
queue_burst_args=()
if [[ -n "${QUEUE_BURST_BATCH_COUNT}" ]]; then
  queue_burst_args+=( --queue-burst-batch-count "${QUEUE_BURST_BATCH_COUNT}" )
fi
if [[ -n "${QUEUE_BURST_MAX_SCHEDULE_SLOT}" ]]; then
  queue_burst_args+=( --queue-burst-max-schedule-slot "${QUEUE_BURST_MAX_SCHEDULE_SLOT}" )
fi
if [[ -n "${QUEUE_BURST_CLOSE_AFTER_RESULTS}" ]]; then
  queue_burst_args+=( --queue-burst-close-after-results "${QUEUE_BURST_CLOSE_AFTER_RESULTS}" )
fi
live_coverage_args=()
if [[ -n "${LIVE_COVERAGE_DIR}" ]]; then
  live_coverage_args=( --live-coverage-dir "${LIVE_COVERAGE_DIR}" )
fi

passed=0
failed=0
skipped=0
xfailed=0
timing=0
seen=0
declare -A covered_modes=()

shopt -s nullglob
iteration_dirs=("${SOURCE_LOG_DIR}"/iter_*)
shopt -u nullglob
if (( ${#iteration_dirs[@]} == 0 )) && [[ -f "${SOURCE_LOG_DIR}/summary.txt" ]]; then
  iteration_dirs=("${SOURCE_LOG_DIR}")
fi

for iter_dir in "${iteration_dirs[@]}"; do
  ((seen += 1))
  iter_name=$(basename "${iter_dir}")
  scenario="${iter_dir}/scenario.toml"
  summary="${iter_dir}/summary.txt"
  source_outcome="${iter_dir}/normalized_outcome.json"

  if [[ ! -f "${scenario}" || ! -f "${summary}" || ( "${TARGET_PAIR}" == "fddev:jito-agave" && ! -f "${source_outcome}" ) ]]; then
    printf 'skipped\t%s\t\t\t\tmissing replay artifacts\n' "${iter_name}" >>"${SUMMARY_TSV}"
    ((skipped += 1))
    continue
  fi

  mode=$(summary_get "${summary}" mode)
  input_family=$(summary_get "${summary}" input_family)
  system_kind=$(summary_get "${summary}" system_kind)

  if [[ "${TARGET_PAIR}" != "fullfd:jito-agave" ]] && ! is_replayable_iteration "${summary}"; then
    printf 'skipped\t%s\t%s\t%s\t%s\tunsupported replay class\n' \
      "${iter_name}" "${mode}" "${input_family}" "${system_kind}" >>"${SUMMARY_TSV}"
    ((skipped += 1))
    continue
  fi

  target_dir="${JITO_ROOT}/${iter_name}"
  fullfd_dir="${FULLFD_ROOT}/${iter_name}"
  comparison="${COMPARISON_ROOT}/${iter_name}.json"

  run_ok=0
  compare_ok=0
  comparison_status=""
  left_outcome="${source_outcome}"
  if [[ "${TARGET_PAIR}" == "fullfd:jito-agave" ]]; then
    left_outcome="${fullfd_dir}/normalized_outcome.json"
    if env "${fullfd_runner_env[@]}" ALLOW_SELF_CHECK_FAILURES="${XFAIL_KNOWN}" \
      "${ROOT}/contrib/run-fullfd-bam-scenario.sh" \
      --fullfd-bin "${FULLFD_BIN}" \
      --config "${FULLFD_CONFIG}" \
      --scenario-file "${scenario}" \
      --source-summary "${summary}" \
      --log-dir "${fullfd_dir}" \
      --rpc-port "${RPC_PORT}" \
      --rpc-url "${FDDEV_RPC_URL}" \
      "${fullfd_solana_args[@]}" \
      --bam-bind "${BAM_BIND}" \
      --bam-url "${BAM_URL}" \
      --bam-bad-url "${BAM_BAD_URL}" \
      --bam-tpu-port "${BAM_TPU_PORT}" \
      --bam-tpu-fwd-port "${BAM_TPU_FWD_PORT}" \
      --bam-shred-port "${BAM_SHRED_PORT}" \
      "${fullfd_check_bam_shred_args[@]}" \
      "${queue_burst_args[@]}" \
      "${live_coverage_args[@]}" \
      --timeout-secs "${TIMEOUT_SECS}"; then
      :
    else
      printf 'failed\t%s\t%s\t%s\t%s\t%s\n' \
        "${iter_name}" "${mode}" "${input_family}" "${system_kind}" "${fullfd_dir}" >>"${SUMMARY_TSV}"
      ((failed += 1))
      build_arg=()
      continue
    fi
    scenario="${fullfd_dir}/scenario.toml"
    summary="${fullfd_dir}/summary.txt"
    if [[ ! -f "${scenario}" || ! -f "${summary}" || ! -f "${left_outcome}" ]]; then
      printf 'failed\t%s\t%s\t%s\t%s\t%s\n' \
        "${iter_name}" "${mode}" "${input_family}" "${system_kind}" "${fullfd_dir}" >>"${SUMMARY_TSV}"
      ((failed += 1))
      build_arg=()
      continue
    fi
    mode=$(summary_get "${summary}" mode)
    input_family=$(summary_get "${summary}" input_family)
    system_kind=$(summary_get "${summary}" system_kind)
    if ! is_replayable_iteration "${summary}"; then
      printf 'skipped\t%s\t%s\t%s\t%s\tfullfd generated unsupported replay class\n' \
        "${iter_name}" "${mode}" "${input_family}" "${system_kind}" >>"${SUMMARY_TSV}"
      ((skipped += 1))
      build_arg=()
      continue
    fi
  fi

  if env "${jito_runner_env[@]}" "${ROOT}/contrib/run-jito-agave-bam-scenario.sh" \
      "${build_arg[@]}" \
      --jito-solana-dir "${JITO_SOLANA_DIR}" \
      --profile "${JITO_PROFILE}" \
      --scenario-file "${scenario}" \
      --source-summary "${summary}" \
      --log-dir "${target_dir}" \
      --rpc-port "${JITO_RPC_PORT}" \
      --bam-bind "${BAM_BIND}" \
      --bam-url "${BAM_URL}" \
      --bam-bad-url "${BAM_BAD_URL}" \
      --tpu-port "${BAM_TPU_PORT}" \
      --tpu-fwd-port "${BAM_TPU_FWD_PORT}" \
      --shred-port "${BAM_SHRED_PORT}" \
      --faucet-port "${FAUCET_PORT}" \
      --timeout-secs "${TIMEOUT_SECS}" \
      "${jito_extra_args[@]}"; then
    run_ok=1
    compare_args=()
    if [[ "${IGNORE_CUS_CONSUMED}" == "1" ]]; then
      compare_args+=( --ignore-cus-consumed )
    fi
    if [[ "${IGNORE_BATCH_RESULT_ORDER}" == "1" ]]; then
      compare_args+=( --ignore-batch-result-order )
    fi
    if [[ "${IGNORE_LOADED_ACCOUNTS_DATA_SIZE}" == "1" ]]; then
      compare_args+=( --ignore-loaded-accounts-data-size )
    fi
    if [[ "${IGNORE_CHAIN_EVIDENCE}" == "1" ]]; then
      compare_args+=( --ignore-chain-evidence )
    fi
    if "${ROOT}/contrib/compare-bam-outcomes.py" \
      "${left_outcome}" \
      "${target_dir}/normalized_outcome.json" \
      "${compare_args[@]}" \
      --output "${comparison}"; then
      compare_ok=1
      comparison_status=$(comparison_status_get "${comparison}")
    fi
  fi

  if [[ "${compare_ok}" == "1" ]]; then
    if [[ "${comparison_status}" == "TIMING_DIFFERENTIAL" ]]; then
      printf 'timing\t%s\t%s\t%s\t%s\tTIMING_DIFFERENTIAL:%s\n' \
        "${iter_name}" "${mode}" "${input_family}" "${system_kind}" "${comparison}" >>"${SUMMARY_TSV}"
      ((timing += 1))
    else
      printf 'passed\t%s\t%s\t%s\t%s\t%s\n' \
        "${iter_name}" "${mode}" "${input_family}" "${system_kind}" "${comparison}" >>"${SUMMARY_TSV}"
      ((passed += 1))
    fi
    covered_modes["${mode}"]=1
  else
    known_id=""
    if [[ "${run_ok}" == "1" ]] && known_id=$(known_failure_id "${mode}" "${input_family}" "${system_kind}" "${comparison}"); then
      printf 'xfailed\t%s\t%s\t%s\t%s\t%s:%s\n' \
        "${iter_name}" "${mode}" "${input_family}" "${system_kind}" "${known_id}" "${comparison}" >>"${SUMMARY_TSV}"
      ((xfailed += 1))
      covered_modes["${mode}"]=1
    else
      printf 'failed\t%s\t%s\t%s\t%s\t%s\n' \
        "${iter_name}" "${mode}" "${input_family}" "${system_kind}" "${target_dir}" >>"${SUMMARY_TSV}"
      ((failed += 1))
    fi
  fi

  build_arg=()
done

coverage_required_modes=""
coverage_missing_modes=""
if [[ "${BASE_SWEEP}" == "1" ]]; then
  coverage_required_modes=$(join_csv "${BASE_SWEEP_MODES[@]}")
  coverage_missing_modes=$(missing_modes covered_modes BASE_SWEEP_MODES)
elif [[ "${CONTROL_PLANE_SWEEP}" == "1" ]]; then
  coverage_required_modes=$(join_csv "${CONTROL_PLANE_SWEEP_MODES[@]}")
  coverage_missing_modes=$(missing_modes covered_modes CONTROL_PLANE_SWEEP_MODES)
elif [[ "${PRE_FULLFD_HARDENING_SWEEP}" == "1" ]]; then
  coverage_required_modes=$(join_csv "${PRE_FULLFD_HARDENING_SWEEP_MODES[@]}")
  coverage_missing_modes=$(missing_modes covered_modes PRE_FULLFD_HARDENING_SWEEP_MODES)
fi

cat >"${LOG_DIR}/summary.txt" <<EOF2
target_pair=${TARGET_PAIR}
source_log_dir=${SOURCE_LOG_DIR}
fddev_config=${FDDEV_CONFIG}
fddev_rpc_url=${FDDEV_RPC_URL}
fullfd_config=${FULLFD_CONFIG}
fullfd_bin=${FULLFD_BIN}
jito_rpc_port=${JITO_RPC_PORT}
bam_url=${BAM_URL}
bam_bind=${BAM_BIND}
bam_tpu_port=${BAM_TPU_PORT}
bam_tpu_fwd_port=${BAM_TPU_FWD_PORT}
bam_shred_port=${BAM_SHRED_PORT}
check_bam_shred=${CHECK_BAM_SHRED}
queue_burst_batch_count=${QUEUE_BURST_BATCH_COUNT}
queue_burst_max_schedule_slot=${QUEUE_BURST_MAX_SCHEDULE_SLOT}
queue_burst_close_after_results=${QUEUE_BURST_CLOSE_AFTER_RESULTS}
fullfd_log_dir=${FULLFD_ROOT}
jito_log_dir=${JITO_ROOT}
comparison_dir=${COMPARISON_ROOT}
iterations_seen=${seen}
passed=${passed}
failed=${failed}
skipped=${skipped}
xfailed=${xfailed}
timing=${timing}
allow_operator_perturbations=${ALLOW_OPERATOR_PERTURBATIONS}
base_sweep=${BASE_SWEEP}
control_plane_sweep=${CONTROL_PLANE_SWEEP}
pre_fullfd_hardening_sweep=${PRE_FULLFD_HARDENING_SWEEP}
ignore_cus_consumed=${IGNORE_CUS_CONSUMED}
ignore_batch_result_order=${IGNORE_BATCH_RESULT_ORDER}
ignore_loaded_accounts_data_size=${IGNORE_LOADED_ACCOUNTS_DATA_SIZE}
ignore_chain_evidence=${IGNORE_CHAIN_EVIDENCE}
coverage_required_modes=${coverage_required_modes}
coverage_missing_modes=${coverage_missing_modes}
summary_tsv=${SUMMARY_TSV}
EOF2

echo "differential summary: passed=${passed} failed=${failed} skipped=${skipped} xfailed=${xfailed} timing=${timing} seen=${seen}"
if [[ -n "${EXPORT_SCENARIO_CORPUS}" ]]; then
  "${ROOT}/contrib/export-bam-diff-corpus.py" \
    --source-log-dir "${SOURCE_LOG_DIR}" \
    --output-dir "${EXPORT_SCENARIO_CORPUS}"
  echo "scenario corpus: ${EXPORT_SCENARIO_CORPUS}"
fi
if [[ "${BASE_SWEEP}" == "1" ]]; then
  if [[ -n "${coverage_missing_modes}" ]]; then
    echo "base sweep coverage missing modes: ${coverage_missing_modes}"
  else
    echo "base sweep coverage: complete"
  fi
elif [[ "${CONTROL_PLANE_SWEEP}" == "1" ]]; then
  if [[ -n "${coverage_missing_modes}" ]]; then
    echo "control-plane sweep coverage missing modes: ${coverage_missing_modes}"
  else
    echo "control-plane sweep coverage: complete"
  fi
elif [[ "${PRE_FULLFD_HARDENING_SWEEP}" == "1" ]]; then
  if [[ -n "${coverage_missing_modes}" ]]; then
    echo "pre-fullfd hardening sweep coverage missing modes: ${coverage_missing_modes}"
  else
    echo "pre-fullfd hardening sweep coverage: complete"
  fi
fi
echo "artifacts: ${LOG_DIR}"

if (( failed > 0 )); then
  exit 1
fi
if [[ "${BASE_SWEEP}" == "1" && -n "${coverage_missing_modes}" ]]; then
  exit 3
fi
if [[ "${CONTROL_PLANE_SWEEP}" == "1" && -n "${coverage_missing_modes}" ]]; then
  exit 3
fi
if [[ "${PRE_FULLFD_HARDENING_SWEEP}" == "1" && -n "${coverage_missing_modes}" ]]; then
  exit 3
fi
if (( passed == 0 && xfailed == 0 && timing == 0 )); then
  exit 2
fi
