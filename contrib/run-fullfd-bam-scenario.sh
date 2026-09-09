#!/usr/bin/env bash

# Run one external BAM scripted scenario against the full Firedancer dev binary.
#
# This adapter lets contrib/fuzz-local-bam-stateful.sh generate target-local
# packets for full Firedancer, then writes a root-level normalized_outcome.json
# so differential wrappers can compare it with other target adapters.

set -euo pipefail
IFS=$'\n\t'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)

FULLFD_BIN="${FULLFD_BIN:-${ROOT}/build/native/gcc/bin/firedancer-dev}"
CONFIG="${FULLFD_CONFIG:-${ROOT}/contrib/local-bam-fullfd-compact.toml}"
LOG_DIR=""
SCENARIO_FILE=""
SOURCE_SUMMARY=""
TARGET_NAME="${TARGET_NAME:-fullfd}"
RPC_PORT="${RPC_PORT:-8899}"
RPC_URL="${RPC_URL:-}"
SOLANA_BIN_DIR="${SOLANA_BIN_DIR:-}"
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
TIMEOUT_SECS="${TIMEOUT_SECS:-150}"
LIVE_COVERAGE_DIR=""

usage() {
  cat <<EOF2
Usage: $(basename "$0") --source-summary PATH [OPTIONS]

Options:
  --fullfd-bin PATH          full Firedancer dev binary
                             (default: build/native/gcc/bin/firedancer-dev)
  --config PATH              full Firedancer BAM config
                             (default: contrib/local-bam-fullfd-compact.toml)
  --log-dir PATH             Artifact directory (default: mktemp)
  --scenario-file PATH       External scripted BAM scenario to replay when
                             source mode is external_scenario
  --source-summary PATH      Source summary.txt containing mode/seed metadata
  --rpc-port PORT            fullfd RPC port (default: 8899)
  --rpc-url URL              fullfd RPC URL (default: http://127.0.0.1:PORT)
  --solana-bin-dir PATH      Directory containing optional Solana CLI helpers
  --bam-bind HOST:PORT       BAM test-server bind address
  --bam-url URL              BAM URL configured into fullfd
  --bam-bad-url URL          Temporary bad BAM URL for URL churn
  --bam-tpu-port PORT        TPU port advertised by BAM test server
  --bam-tpu-fwd-port PORT    TPU forward port advertised by BAM
  --bam-shred-port PORT      Shred port advertised by BAM
  --check-bam-shred          Capture the advertised BAM shred UDP port
                             and require at least one packet
  --queue-burst-batch-count N
                             Override queue-burst helper batch count
  --queue-burst-max-schedule-slot VALUE
                             Override queue-burst max_schedule_slot
  --queue-burst-close-after-results N
                             Override queue-burst reconnect point
  --target-name NAME         Outcome target label (default: fullfd)
  --timeout-secs N           Wait timeout (default: 150)
  --live-coverage-dir PATH   Write LLVM profiles from the live full validator
  -h, --help                 Show this help
EOF2
}

die() {
  echo "error: $*" >&2
  exit 1
}

summary_get() {
  local key="$1"
  awk -F= -v key="${key}" '$1 == key { print substr($0, length(key) + 2); exit }' "${SOURCE_SUMMARY}"
}

while (($#)); do
  case "$1" in
    --fullfd-bin)
      FULLFD_BIN="$2"
      shift 2
      ;;
    --config)
      CONFIG="$2"
      shift 2
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
    --target-name)
      TARGET_NAME="$2"
      shift 2
      ;;
    --timeout-secs)
      TIMEOUT_SECS="$2"
      shift 2
      ;;
    --live-coverage-dir)
      LIVE_COVERAGE_DIR="$2"
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

[[ -n "${SOURCE_SUMMARY}" ]] || die "--source-summary is required"
[[ -x "${FULLFD_BIN}" ]] || die "missing ${FULLFD_BIN}; build firedancer-dev first"
[[ -f "${CONFIG}" ]] || die "missing fullfd config ${CONFIG}"
[[ -f "${SOURCE_SUMMARY}" ]] || die "missing source summary ${SOURCE_SUMMARY}"
[[ -x "${ROOT}/contrib/fuzz-local-bam-stateful.sh" ]] || die "missing fuzz-local-bam-stateful.sh"

MODE_VALUE=$(summary_get mode)
INPUT_FAMILY_VALUE=$(summary_get input_family)
SEED_VALUE=$(summary_get seed)
REQUESTED_KUNORPUS_SYSTEM_KIND=$(summary_get requested_kunorpus_system_kind)
KUNORPUS_COUNT_VALUE=$(summary_get kunorpus_count)
KUNORPUS_SEED_WINDOW_VALUE=$(summary_get kunorpus_seed_window)
KUNORPUS_MAX_TRANSFER_LAMPORTS_VALUE=$(summary_get kunorpus_max_transfer_lamports)
GENERATED_BUNDLE_MAX_BATCHES_VALUE=$(summary_get generated_bundle_max_batches)
GENERATED_BUNDLE_MAX_PACKETS_VALUE=$(summary_get generated_bundle_max_packets)
if [[ "${MODE_VALUE}" == "external_scenario" ]]; then
  [[ -n "${SCENARIO_FILE}" ]] || die "--scenario-file is required for external_scenario"
  [[ -f "${SCENARIO_FILE}" ]] || die "missing scenario file ${SCENARIO_FILE}"
fi
if [[ -z "${SEED_VALUE}" ]]; then
  SEED_VALUE=1
fi
if [[ -z "${INPUT_FAMILY_VALUE}" ]]; then
  if [[ "${MODE_VALUE}" == "external_scenario" ]]; then
    INPUT_FAMILY_VALUE=external_scenario
  else
    INPUT_FAMILY_VALUE=synthetic
  fi
fi
if [[ -z "${REQUESTED_KUNORPUS_SYSTEM_KIND}" ]]; then
  REQUESTED_KUNORPUS_SYSTEM_KIND=any
fi
KUNORPUS_COUNT_VALUE=${KUNORPUS_COUNT_VALUE:-64}
KUNORPUS_SEED_WINDOW_VALUE=${KUNORPUS_SEED_WINDOW_VALUE:-16}
KUNORPUS_MAX_TRANSFER_LAMPORTS_VALUE=${KUNORPUS_MAX_TRANSFER_LAMPORTS_VALUE:-50000000000000}
GENERATED_BUNDLE_MAX_BATCHES_VALUE=${GENERATED_BUNDLE_MAX_BATCHES_VALUE:-8}
GENERATED_BUNDLE_MAX_PACKETS_VALUE=${GENERATED_BUNDLE_MAX_PACKETS_VALUE:-5}

if [[ -z "${LOG_DIR}" ]]; then
  LOG_DIR=$(mktemp -d /tmp/firebam-fullfd-scenario.XXXXXX)
fi
mkdir -p "${LOG_DIR}"

WORK_DIR="${LOG_DIR}/.fullfd-run"
rm -rf -- "${WORK_DIR}"

solana_bin_args=()
if [[ -n "${SOLANA_BIN_DIR}" ]]; then
  solana_bin_args=( --solana-bin-dir "${SOLANA_BIN_DIR}" )
fi

fuzz_args=(
  --target-name "${TARGET_NAME}" \
  --runner-kind fullfd \
  --fddev-bin "${FULLFD_BIN}" \
  --config "${CONFIG}" \
  --rpc-url "${RPC_URL}" \
  "${solana_bin_args[@]}" \
  --bam-bind "${BAM_BIND}" \
  --bam-url "${BAM_URL}" \
  --bam-bad-url "${BAM_BAD_URL}" \
  --bam-tpu-port "${BAM_TPU_PORT}" \
  --bam-tpu-fwd-port "${BAM_TPU_FWD_PORT}" \
  --bam-shred-port "${BAM_SHRED_PORT}" \
  --iterations 1 \
  --seed "${SEED_VALUE}" \
  --mode "${MODE_VALUE}" \
  --input-family "${INPUT_FAMILY_VALUE}" \
  --kunorpus-count "${KUNORPUS_COUNT_VALUE}" \
  --kunorpus-seed-window "${KUNORPUS_SEED_WINDOW_VALUE}" \
  --kunorpus-max-transfer-lamports "${KUNORPUS_MAX_TRANSFER_LAMPORTS_VALUE}" \
  --kunorpus-system-kind "${REQUESTED_KUNORPUS_SYSTEM_KIND}" \
  --generated-bundle-max-batches "${GENERATED_BUNDLE_MAX_BATCHES_VALUE}" \
  --generated-bundle-max-packets "${GENERATED_BUNDLE_MAX_PACKETS_VALUE}" \
  --timeout-secs "${TIMEOUT_SECS}" \
  --log-dir "${WORK_DIR}"
)

if [[ "${CHECK_BAM_SHRED}" == "1" ]]; then
  fuzz_args+=( --check-bam-shred )
fi

if [[ "${MODE_VALUE}" == "external_scenario" ]]; then
  fuzz_args+=( --scenario-file "${SCENARIO_FILE}" )
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

"${ROOT}/contrib/fuzz-local-bam-stateful.sh" "${fuzz_args[@]}"

shopt -s nullglob
iter_dirs=("${WORK_DIR}"/iter_*)
shopt -u nullglob
(( ${#iter_dirs[@]} == 1 )) || die "expected one fullfd iteration under ${WORK_DIR}, found ${#iter_dirs[@]}"

iter_dir="${iter_dirs[0]}"
for path in "${iter_dir}"/*; do
  cp -a -- "${path}" "${LOG_DIR}/"
done

cat >"${LOG_DIR}/adapter-summary.txt" <<EOF2
target=${TARGET_NAME}
runner_kind=fullfd
mode=${MODE_VALUE}
input_family=${INPUT_FAMILY_VALUE}
scenario_file=${SCENARIO_FILE}
source_summary=${SOURCE_SUMMARY}
requested_kunorpus_system_kind=${REQUESTED_KUNORPUS_SYSTEM_KIND}
kunorpus_count=${KUNORPUS_COUNT_VALUE}
kunorpus_seed_window=${KUNORPUS_SEED_WINDOW_VALUE}
kunorpus_max_transfer_lamports=${KUNORPUS_MAX_TRANSFER_LAMPORTS_VALUE}
generated_bundle_max_batches=${GENERATED_BUNDLE_MAX_BATCHES_VALUE}
generated_bundle_max_packets=${GENERATED_BUNDLE_MAX_PACKETS_VALUE}
fullfd_bin=${FULLFD_BIN}
config=${CONFIG}
rpc_url=${RPC_URL}
bam_url=${BAM_URL}
check_bam_shred=${CHECK_BAM_SHRED}
queue_burst_batch_count=${QUEUE_BURST_BATCH_COUNT}
queue_burst_max_schedule_slot=${QUEUE_BURST_MAX_SCHEDULE_SLOT}
queue_burst_close_after_results=${QUEUE_BURST_CLOSE_AFTER_RESULTS}
work_dir=${WORK_DIR}
EOF2

echo "validated: fullfd BAM scenario produced a normalized outcome"
echo "artifacts: ${LOG_DIR}"
