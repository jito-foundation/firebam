#!/usr/bin/env bash

# Validates that the local BAM controller reaches a live fddev node and that
# scenario-specific evidence appears in the controller/validator logs.

set -euo pipefail
IFS=$'\n\t'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)

CONFIG="${ROOT}/contrib/local-bam-compact.toml"
FD_BIN="${FD_BIN:-${FDDEV:-${ROOT}/build/native/gcc/bin/fddev}}"
CONTROL_BIN="${CONTROL_BIN:-${FDCTL:-${ROOT}/build/native/gcc/bin/fdctl}}"
SCENARIO="benign"
SCENARIO_FILE=""
OPERATOR_EVENTS=""
TIMEOUT_SECS=60
LOG_DIR=""
SKIP_CONFIGURE=0
TXN_LOOP=0
USE_SUDO="${USE_SUDO:-1}"
EXPECT_BAM_PATTERNS=()
EXPECT_FD_PATTERNS=()
EXPECT_OPERATOR_PATTERNS=()
EXPECT_BAM_COUNT_PATTERNS=()
EXPECT_BAM_COUNT_MINS=()
EXPECT_FD_COUNT_PATTERNS=()
EXPECT_FD_COUNT_MINS=()
EXPECT_OPERATOR_COUNT_PATTERNS=()
EXPECT_OPERATOR_COUNT_MINS=()
BAM_BIND="${BAM_BIND:-127.0.0.1:50055}"
BAM_TPU_IP="${BAM_TPU_IP:-127.0.0.1}"
BAM_TPU_PORT="${BAM_TPU_PORT:-9007}"
BAM_TPU_FWD_IP="${BAM_TPU_FWD_IP:-127.0.0.1}"
BAM_TPU_FWD_PORT="${BAM_TPU_FWD_PORT:-9008}"
BAM_SHRED_IP="${BAM_SHRED_IP:-127.0.0.1}"
BAM_SHRED_PORT="${BAM_SHRED_PORT:-9009}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --scenario NAME        BAM test-server scenario (default: benign)
  --scenario-file PATH   Scripted BAM scenario file (.toml or .json)
  --operator-events PATH Operator event file for fdctl set-bam perturbations
  --expect-bam-pattern RE Require an extra regex match in bam.log
  --expect-fd-pattern RE  Require an extra regex match in fd.log
  --expect-operator-pattern RE Require an extra regex match in operator.log
  --expect-bam-count RE N Require bam.log to match regex at least N times
  --expect-fd-count RE N  Require fd.log to match regex at least N times
  --expect-operator-count RE N Require operator.log to match regex at least N times
  --config PATH          Validator config path (default: contrib/local-bam-compact.toml)
  --fd-bin PATH          Validator binary (default: build/native/gcc/bin/fddev)
  --control-bin PATH     Control binary for operator events (default: build/native/gcc/bin/fdctl)
  --bam-bind HOST:PORT   BAM test-server bind address (default: 127.0.0.1:50055)
  --bam-tpu-port PORT    TPU port advertised by BAM (default: 9007)
  --bam-tpu-fwd-port PORT TPU-forward port advertised by BAM (default: 9008)
  --bam-shred-port PORT  Shred port advertised by BAM (default: 9009)
  --timeout-secs N       Validation timeout in seconds (default: 60)
  --log-dir PATH         Artifact directory (default: mktemp)
  --skip-configure       Skip validator 'configure check all'
  --txn-loop             Start a background validator 'txn' loop during validation
  -h, --help             Show this help
EOF
}

while (($#)); do
  case "$1" in
    --scenario)
      SCENARIO="$2"
      shift 2
      ;;
    --scenario-file)
      SCENARIO_FILE="$2"
      shift 2
      ;;
    --operator-events)
      OPERATOR_EVENTS="$2"
      shift 2
      ;;
    --expect-bam-pattern)
      EXPECT_BAM_PATTERNS+=( "$2" )
      shift 2
      ;;
    --expect-fd-pattern)
      EXPECT_FD_PATTERNS+=( "$2" )
      shift 2
      ;;
    --expect-operator-pattern)
      EXPECT_OPERATOR_PATTERNS+=( "$2" )
      shift 2
      ;;
    --expect-bam-count)
      EXPECT_BAM_COUNT_PATTERNS+=( "$2" )
      EXPECT_BAM_COUNT_MINS+=( "$3" )
      shift 3
      ;;
    --expect-fd-count)
      EXPECT_FD_COUNT_PATTERNS+=( "$2" )
      EXPECT_FD_COUNT_MINS+=( "$3" )
      shift 3
      ;;
    --expect-operator-count)
      EXPECT_OPERATOR_COUNT_PATTERNS+=( "$2" )
      EXPECT_OPERATOR_COUNT_MINS+=( "$3" )
      shift 3
      ;;
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
    --timeout-secs)
      TIMEOUT_SECS="$2"
      shift 2
      ;;
    --log-dir)
      LOG_DIR="$2"
      shift 2
      ;;
    --skip-configure)
      SKIP_CONFIGURE=1
      shift
      ;;
    --txn-loop)
      TXN_LOOP=1
      shift
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

BAM_MANIFEST="${ROOT}/contrib/bam-test-server/Cargo.toml"

die() {
  echo "error: $*" >&2
  exit 1
}

[[ -x "${FD_BIN}" ]] || die "missing validator binary ${FD_BIN}"
if [[ -n "${OPERATOR_EVENTS}" ]]; then
  [[ -x "${CONTROL_BIN}" ]] || die "missing control binary ${CONTROL_BIN}"
fi
[[ -f "${BAM_MANIFEST}" ]] || die "missing ${BAM_MANIFEST}"
[[ -f "${CONFIG}" ]] || die "missing config ${CONFIG}"
[[ -z "${SCENARIO_FILE}" || -f "${SCENARIO_FILE}" ]] || die "missing scenario file ${SCENARIO_FILE}"
[[ -z "${OPERATOR_EVENTS}" || -f "${OPERATOR_EVENTS}" ]] || die "missing operator events ${OPERATOR_EVENTS}"
command -v cargo >/dev/null 2>&1 || die "cargo not found"

SUDO=()
if [[ "${USE_SUDO}" != "0" && "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo not found"
  SUDO=( sudo )
fi

if [[ -z "${LOG_DIR}" ]]; then
  LOG_DIR=$(mktemp -d /tmp/firebam-live-smoke.XXXXXX)
fi
mkdir -p "${LOG_DIR}"

BAM_LOG="${LOG_DIR}/bam.log"
FD_LOG="${LOG_DIR}/fd.log"
TXN_LOG="${LOG_DIR}/txn.log"
OPERATOR_LOG="${LOG_DIR}/operator.log"
FD_PID_FILE="${LOG_DIR}/fd.pid"
BAM_PID_FILE="${LOG_DIR}/bam.pid"
BAM_START_SCRIPT="${LOG_DIR}/start-bam.sh"
VALIDATOR_PROFILE_PATTERN="${VALIDATOR_PROFILE_PATTERN:-}"

read_pid_file() {
  local pid_file="$1"
  [[ -f "${pid_file}" ]] || return 1
  tr -d '[:space:]' < "${pid_file}"
}

current_fd_pid() {
  if pid=$(read_pid_file "${FD_PID_FILE}" 2>/dev/null); then
    printf '%s\n' "${pid}"
    return 0
  fi
  [[ -n "${FD_PID:-}" ]] || return 1
  printf '%s\n' "${FD_PID}"
}

current_bam_pid() {
  if pid=$(read_pid_file "${BAM_PID_FILE}" 2>/dev/null); then
    printf '%s\n' "${pid}"
    return 0
  fi
  [[ -n "${BAM_PID:-}" ]] || return 1
  printf '%s\n' "${BAM_PID}"
}

cleanup() {
  trap - EXIT INT TERM
  set +e
  [[ -n "${TXN_PID:-}" ]] && kill "${TXN_PID}" 2>/dev/null || true
  [[ -n "${OPERATOR_PID:-}" ]] && kill "${OPERATOR_PID}" 2>/dev/null || true
  if fd_pid=$(current_fd_pid 2>/dev/null); then
    "${ROOT}/contrib/dump-live-validator-coverage.sh" \
      --pid "${fd_pid}" \
      --profile-pattern "${VALIDATOR_PROFILE_PATTERN}" \
      --phase final || true
    "${SUDO[@]}" kill -- "-${fd_pid}" 2>/dev/null || true
  fi
  [[ -n "${FD_SUPERVISOR_PID:-}" ]] && kill "${FD_SUPERVISOR_PID}" 2>/dev/null || true
  if bam_pid=$(current_bam_pid 2>/dev/null); then
    kill "${bam_pid}" 2>/dev/null || true
  fi
  [[ -n "${TXN_PID:-}" ]] && wait "${TXN_PID}" 2>/dev/null || true
  [[ -n "${OPERATOR_PID:-}" ]] && wait "${OPERATOR_PID}" 2>/dev/null || true
  [[ -n "${FD_SUPERVISOR_PID:-}" ]] && wait "${FD_SUPERVISOR_PID}" 2>/dev/null || true
  [[ -n "${BAM_PID:-}" ]] && wait "${BAM_PID}" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

if [[ "${SKIP_CONFIGURE}" == "0" ]]; then
  "${SUDO[@]}" "${FD_BIN}" configure check all --config "${CONFIG}" >/dev/null
fi

echo "logs: ${LOG_DIR}"
echo "scenario: ${SCENARIO}"
[[ -n "${SCENARIO_FILE}" ]] && echo "scenario file: ${SCENARIO_FILE}"
[[ -n "${OPERATOR_EVENTS}" ]] && echo "operator events: ${OPERATOR_EVENTS}"
echo "config: ${CONFIG}"
echo "validator binary: ${FD_BIN}"
[[ -n "${OPERATOR_EVENTS}" ]] && echo "control binary: ${CONTROL_BIN}"
echo "bam bind: ${BAM_BIND}"

BAM_CMD=(
  cargo run --manifest-path "${BAM_MANIFEST}" --bin bam-test-server --
  --scenario "${SCENARIO}"
  --bind "${BAM_BIND}"
  --tpu-ip "${BAM_TPU_IP}"
  --tpu-port "${BAM_TPU_PORT}"
  --tpu-fwd-ip "${BAM_TPU_FWD_IP}"
  --tpu-fwd-port "${BAM_TPU_FWD_PORT}"
  --shred-ip "${BAM_SHRED_IP}"
  --shred-port "${BAM_SHRED_PORT}"
)
if [[ -n "${SCENARIO_FILE}" ]]; then
  BAM_CMD+=( --scenario-file "${SCENARIO_FILE}" )
fi

{
  printf '%s\n' '#!/usr/bin/env bash'
  printf '%s\n' 'set -euo pipefail'
  printf '%s\n\n' "IFS=\$'\n\t'"
  printf 'exec'
  for arg in "${BAM_CMD[@]}"; do
    printf ' %q' "${arg}"
  done
  printf '\n'
} > "${BAM_START_SCRIPT}"
chmod +x "${BAM_START_SCRIPT}"

rm -f "${BAM_PID_FILE}"
"${BAM_START_SCRIPT}" >"${BAM_LOG}" 2>&1 &
BAM_PID=$!
printf '%s\n' "${BAM_PID}" > "${BAM_PID_FILE}"

rm -f "${FD_PID_FILE}"
VALIDATOR_ENV=()
if [[ -n "${VALIDATOR_PROFILE_PATTERN}" ]]; then
  VALIDATOR_ENV=( env "LLVM_PROFILE_FILE=${VALIDATOR_PROFILE_PATTERN}" )
fi
"${SUDO[@]}" "${VALIDATOR_ENV[@]}" bash -lc '
  pid_file="$1"
  fddev_bin="$2"
  config_path="$3"
  log_file="$4"
  setsid "${fddev_bin}" dev --no-watch --no-configure --config "${config_path}" > "${log_file}" 2>&1 &
  child_pid=$!
  echo "${child_pid}" > "${pid_file}"
  wait "${child_pid}"
' bash "${FD_PID_FILE}" "${FD_BIN}" "${CONFIG}" "${FD_LOG}" &
FD_SUPERVISOR_PID=$!

for _ in $(seq 1 50); do
  if [[ -s "${FD_PID_FILE}" ]]; then
    FD_PID=$(tr -d '[:space:]' < "${FD_PID_FILE}")
    break
  fi
  sleep 0.2
done
[[ -n "${FD_PID:-}" ]] || die "failed to capture fddev pid"

if [[ "${TXN_LOOP}" == "1" ]]; then
  (
    while true; do
      "${FD_BIN}" txn --config "${CONFIG}" --count 32
      sleep 0.2
    done
  ) >"${TXN_LOG}" 2>&1 &
  TXN_PID=$!
fi

if [[ -n "${OPERATOR_EVENTS}" ]]; then
  "${ROOT}/contrib/run-bam-operator-events.sh" \
    --config "${CONFIG}" \
    --control-bin "${CONTROL_BIN}" \
    --fd-bin "${FD_BIN}" \
    --events "${OPERATOR_EVENTS}" \
    --fd-log "${FD_LOG}" \
    --bam-log "${BAM_LOG}" \
    --fd-pid "${FD_PID}" \
    --fd-pid-file "${FD_PID_FILE}" \
    --bam-pid "${BAM_PID}" \
    --bam-pid-file "${BAM_PID_FILE}" \
    --bam-start-script "${BAM_START_SCRIPT}" \
    >"${OPERATOR_LOG}" 2>&1 &
  OPERATOR_PID=$!
fi

child_alive() {
  local pid="$1"
  kill -0 "${pid}" 2>/dev/null
}

if ! child_alive "${BAM_PID}"; then
  echo "bam-test-server exited before validation started" >&2
  tail -n 80 "${BAM_LOG}" >&2 || true
  exit 1
fi

if ! child_alive "${FD_SUPERVISOR_PID}"; then
  echo "fddev exited before validation started" >&2
  tail -n 80 "${FD_LOG}" >&2 || true
  exit 1
fi

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

wait_for_count_at_least() {
  local file="$1"
  local pattern="$2"
  local minimum="$3"
  local deadline="$4"
  local count
  while :; do
    count=$(grep -cE "${pattern}" "${file}" 2>/dev/null || true)
    if (( count >= minimum )); then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 1
  done
  return 1
}

require_pattern() {
  local file="$1"
  local pattern="$2"
  local description="$3"
  local deadline="$4"
  if wait_for_pattern "${file}" "${pattern}" "${deadline}"; then
    echo "ok: ${description}"
  else
    echo "missing: ${description}" >&2
    echo "--- tail ${file} ---" >&2
    tail -n 80 "${file}" >&2 || true
    exit 1
  fi
}

require_count_at_least() {
  local file="$1"
  local pattern="$2"
  local minimum="$3"
  local description="$4"
  local deadline="$5"
  local count
  if wait_for_count_at_least "${file}" "${pattern}" "${minimum}" "${deadline}"; then
    count=$(grep -cE "${pattern}" "${file}" 2>/dev/null || true)
    echo "ok: ${description} (count=${count})"
  else
    echo "missing: ${description}" >&2
    echo "--- tail ${file} ---" >&2
    tail -n 80 "${file}" >&2 || true
    exit 1
  fi
}

require_any_pattern() {
  local file="$1"
  local description="$2"
  local deadline="$3"
  shift 3
  local patterns=( "$@" )
  local pattern
  while (( SECONDS < deadline )); do
    for pattern in "${patterns[@]}"; do
      if grep -qE "${pattern}" "${file}" 2>/dev/null; then
        echo "ok: ${description}"
        return 0
      fi
    done
    sleep 1
  done

  echo "missing: ${description}" >&2
  echo "--- tail ${file} ---" >&2
  tail -n 80 "${file}" >&2 || true
  exit 1
}

DEADLINE=$((SECONDS + TIMEOUT_SECS))

require_pattern "${BAM_LOG}" 'GetAuthChallenge:' 'validator requested BAM auth challenge' "${DEADLINE}"
require_pattern "${BAM_LOG}" 'GetBuilderConfig:' 'validator requested BAM config' "${DEADLINE}"
require_pattern "${BAM_LOG}" 'InitSchedulerStream:' 'validator opened scheduler stream' "${DEADLINE}"
require_pattern "${BAM_LOG}" 'scheduler<-validator auth proof' 'validator sent scheduler auth proof' "${DEADLINE}"
require_pattern "${FD_LOG}" 'BAM identity pubkey updated to' 'validator initialized BAM identity' "${DEADLINE}"
require_any_pattern \
  "${FD_LOG}" \
  'validator applied BAM TPU config' \
  "${DEADLINE}" \
  'Updated TPU addresses:' \
  'Using configured default TPU addresses:'

if [[ -n "${SCENARIO_FILE}" ]]; then
  require_pattern "${FD_LOG}" 'BAM rx bundle:' 'validator decoded at least one BAM bundle' "${DEADLINE}"
  if wait_for_pattern "${BAM_LOG}" 'scheduler<-validator batch results count=|scheduler<-validator leader state' "${DEADLINE}"; then
    echo "ok: validator published BAM feedback"
  else
    echo "note: scripted scenario reached live decode, but no BAM feedback arrived within ${TIMEOUT_SECS}s"
  fi
else
  case "${SCENARIO}" in
    benign|h01-config-redirect|h03-bam-override)
      require_any_pattern \
        "${BAM_LOG}" \
        'validator published BAM feedback' \
        "${DEADLINE}" \
        'scheduler<-validator leader state' \
        'scheduler<-validator batch results count='
      require_pattern "${FD_LOG}" 'BAM rx bundle:' 'validator decoded at least one BAM bundle' "${DEADLINE}"
      ;;
    h02-seq-id-overrun)
      require_pattern "${BAM_LOG}" 'sending H02 duplicate seq_id=' 'controller sent the duplicate-seq-id overrun payload' "${DEADLINE}"
      require_pattern "${FD_LOG}" 'BAM rx bundle: seq_id=.*txns=5 mode=atomic' 'validator decoded the first duplicate-seq-id bundle fragment' "${DEADLINE}"
      require_pattern "${FD_LOG}" 'BAM rx bundle: seq_id=.*txns=1 mode=atomic' 'validator decoded the second duplicate-seq-id bundle fragment' "${DEADLINE}"
      if wait_for_pattern "${BAM_LOG}" 'scheduler<-validator batch results count=|scheduler<-validator leader state' "${DEADLINE}"; then
        echo "ok: validator returned BAM feedback for the duplicate-seq-id scenario"
      else
        echo "note: duplicate-seq-id scenario reached live decode, but no terminal BAM feedback arrived within ${TIMEOUT_SECS}s"
      fi
      ;;
    c01-auth-abort)
      require_pattern "${FD_LOG}" 'fd_keyguard_payload_authorize failed|SIGABRT|ambiguous payload type' 'validator hit the expected auth-abort path' "${DEADLINE}"
      ;;
    *)
      require_pattern "${FD_LOG}" 'BAM rx bundle:' 'validator decoded at least one BAM bundle' "${DEADLINE}"
      ;;
  esac
fi

if [[ -n "${OPERATOR_EVENTS}" ]]; then
  require_pattern "${OPERATOR_LOG}" '^operator:' 'operator runner executed at least one operator event' "${DEADLINE}"
  if ! wait "${OPERATOR_PID}"; then
    echo "operator runner failed" >&2
    echo "--- tail ${OPERATOR_LOG} ---" >&2
    tail -n 80 "${OPERATOR_LOG}" >&2 || true
    exit 1
  fi
  OPERATOR_PID=""
fi

if ((${#EXPECT_BAM_PATTERNS[@]})); then
  for pattern in "${EXPECT_BAM_PATTERNS[@]}"; do
    require_pattern "${BAM_LOG}" "${pattern}" "bam.log matched extra expectation: ${pattern}" "${DEADLINE}"
  done
fi

if ((${#EXPECT_FD_PATTERNS[@]})); then
  for pattern in "${EXPECT_FD_PATTERNS[@]}"; do
    require_pattern "${FD_LOG}" "${pattern}" "fd.log matched extra expectation: ${pattern}" "${DEADLINE}"
  done
fi

if ((${#EXPECT_OPERATOR_PATTERNS[@]})); then
  for pattern in "${EXPECT_OPERATOR_PATTERNS[@]}"; do
    require_pattern "${OPERATOR_LOG}" "${pattern}" "operator.log matched extra expectation: ${pattern}" "${DEADLINE}"
  done
fi

if ((${#EXPECT_BAM_COUNT_PATTERNS[@]})); then
  for idx in "${!EXPECT_BAM_COUNT_PATTERNS[@]}"; do
    require_count_at_least \
      "${BAM_LOG}" \
      "${EXPECT_BAM_COUNT_PATTERNS[idx]}" \
      "${EXPECT_BAM_COUNT_MINS[idx]}" \
      "bam.log matched ${EXPECT_BAM_COUNT_PATTERNS[idx]} at least ${EXPECT_BAM_COUNT_MINS[idx]} times" \
      "${DEADLINE}"
  done
fi

if ((${#EXPECT_FD_COUNT_PATTERNS[@]})); then
  for idx in "${!EXPECT_FD_COUNT_PATTERNS[@]}"; do
    require_count_at_least \
      "${FD_LOG}" \
      "${EXPECT_FD_COUNT_PATTERNS[idx]}" \
      "${EXPECT_FD_COUNT_MINS[idx]}" \
      "fd.log matched ${EXPECT_FD_COUNT_PATTERNS[idx]} at least ${EXPECT_FD_COUNT_MINS[idx]} times" \
      "${DEADLINE}"
  done
fi

if ((${#EXPECT_OPERATOR_COUNT_PATTERNS[@]})); then
  for idx in "${!EXPECT_OPERATOR_COUNT_PATTERNS[@]}"; do
    require_count_at_least \
      "${OPERATOR_LOG}" \
      "${EXPECT_OPERATOR_COUNT_PATTERNS[idx]}" \
      "${EXPECT_OPERATOR_COUNT_MINS[idx]}" \
      "operator.log matched ${EXPECT_OPERATOR_COUNT_PATTERNS[idx]} at least ${EXPECT_OPERATOR_COUNT_MINS[idx]} times" \
      "${DEADLINE}"
  done
fi

echo "validation passed"
echo "artifacts: ${LOG_DIR}"
