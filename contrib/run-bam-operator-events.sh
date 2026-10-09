#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)

CONFIG="${ROOT}/contrib/local-bam-compact.toml"
EVENTS=""
CONTROL_BIN="${CONTROL_BIN:-${FDCTL:-${ROOT}/build/native/gcc/bin/fdctl}}"
FD_BIN="${FD_BIN:-${FDDEV:-${ROOT}/build/native/gcc/bin/fddev}}"
USE_SUDO="${USE_SUDO:-1}"
FD_LOG=""
BAM_LOG=""
FD_PID=""
BAM_PID=""
FD_PID_FILE=""
BAM_PID_FILE=""
BAM_START_SCRIPT=""
VALIDATOR_PROFILE_PATTERN="${VALIDATOR_PROFILE_PATTERN:-}"

usage() {
  cat <<EOF
Usage: $(basename "$0") --events FILE [--config PATH] [--control-bin PATH] [--fd-bin PATH] [--fd-log PATH] [--bam-log PATH] [--fd-pid PID] [--fd-pid-file PATH] [--bam-pid PID] [--bam-pid-file PATH] [--bam-start-script PATH]

Event file syntax:
  sleep_ms <milliseconds>
  wait_log <fd|bam> <timeout_ms> <regex>
  wait_log_growth <fd|bam> <timeout_ms> <regex>
  wait_log_count <fd|bam> <timeout_ms> <minimum> <regex>
  signal <fd|bam> <stop|cont|term|kill|int>
  restart_fd
  restart_bam
  set_bam [--enable|--disable] [--url URL] [--sni SNI]
  set_bam_expect_fail [--enable|--disable] [--url URL] [--sni SNI]
  set_bam_clear_url
  set_bam_clear_sni
  set_identity <keypair> [control-binary set-identity flags...]

Blank lines and lines starting with '#' are ignored.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

while (($#)); do
  case "$1" in
    --events)
      EVENTS="$2"
      shift 2
      ;;
    --config)
      CONFIG="$2"
      shift 2
      ;;
    --control-bin)
      CONTROL_BIN="$2"
      shift 2
      ;;
    --fd-bin)
      FD_BIN="$2"
      shift 2
      ;;
    --fd-log)
      FD_LOG="$2"
      shift 2
      ;;
    --bam-log)
      BAM_LOG="$2"
      shift 2
      ;;
    --fd-pid)
      FD_PID="$2"
      shift 2
      ;;
    --fd-pid-file)
      FD_PID_FILE="$2"
      shift 2
      ;;
    --bam-pid)
      BAM_PID="$2"
      shift 2
      ;;
    --bam-pid-file)
      BAM_PID_FILE="$2"
      shift 2
      ;;
    --bam-start-script)
      BAM_START_SCRIPT="$2"
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

[[ -n "${EVENTS}" ]] || die "missing --events"
[[ -f "${EVENTS}" ]] || die "missing events file ${EVENTS}"
[[ -f "${CONFIG}" ]] || die "missing config ${CONFIG}"
[[ -x "${CONTROL_BIN}" ]] || die "missing control binary ${CONTROL_BIN}"
[[ -x "${FD_BIN}" ]] || die "missing validator binary ${FD_BIN}"

SUDO=()
if [[ "${USE_SUDO}" != "0" && "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo not found"
  SUDO=( sudo )
fi

sleep_ms() {
  local total_ms="$1"
  local secs=$(( total_ms / 1000 ))
  local rem_ms=$(( total_ms % 1000 ))
  sleep "${secs}.$(printf '%03d' "${rem_ms}")"
}

read_pid_file() {
  local pid_file="$1"
  [[ -n "${pid_file}" && -f "${pid_file}" ]] || return 1
  tr -d '[:space:]' < "${pid_file}"
}

write_pid_file() {
  local pid_file="$1"
  local pid="$2"
  [[ -n "${pid_file}" ]] || return 1
  printf '%s\n' "${pid}" > "${pid_file}"
}

require_target_log() {
  local target="$1"
  case "${target}" in
    fd)
      [[ -n "${FD_LOG}" ]] || die "fd log path required for '${target}' events"
      [[ -f "${FD_LOG}" ]] || die "fd log does not exist: ${FD_LOG}"
      printf '%s\n' "${FD_LOG}"
      ;;
    bam)
      [[ -n "${BAM_LOG}" ]] || die "bam log path required for '${target}' events"
      [[ -f "${BAM_LOG}" ]] || die "bam log does not exist: ${BAM_LOG}"
      printf '%s\n' "${BAM_LOG}"
      ;;
    *)
      die "unsupported log target '${target}'"
      ;;
  esac
}

require_target_pid() {
  local target="$1"
  case "${target}" in
    fd)
      if pid=$(read_pid_file "${FD_PID_FILE}" 2>/dev/null); then
        FD_PID="${pid}"
      fi
      [[ -n "${FD_PID}" ]] || die "fd pid required for '${target}' events"
      printf '%s\n' "${FD_PID}"
      ;;
    bam)
      if pid=$(read_pid_file "${BAM_PID_FILE}" 2>/dev/null); then
        BAM_PID="${pid}"
      fi
      [[ -n "${BAM_PID}" ]] || die "bam pid required for '${target}' events"
      printf '%s\n' "${BAM_PID}"
      ;;

    *)
      die "unsupported pid target '${target}'"
      ;;
  esac
}

wait_pid_exit() {
  local pid="$1"
  local timeout_secs="$2"
  local deadline=$(( SECONDS + timeout_secs ))
  while :; do
    if ! "${SUDO[@]}" kill -0 "${pid}" 2>/dev/null; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 0.1
  done
  return 1
}

wait_log_pattern() {
  local file="$1"
  local timeout_ms="$2"
  local pattern="$3"
  local deadline=$(( SECONDS + ((timeout_ms + 999) / 1000) ))
  while :; do
    if grep -qE "${pattern}" "${file}" 2>/dev/null; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 0.1
  done
  return 1
}

wait_log_growth() {
  local file="$1"
  local timeout_ms="$2"
  local pattern="$3"
  local baseline
  baseline=$(grep -cE "${pattern}" "${file}" 2>/dev/null || true)
  local deadline=$(( SECONDS + ((timeout_ms + 999) / 1000) ))
  local count
  while :; do
    count=$(grep -cE "${pattern}" "${file}" 2>/dev/null || true)
    if (( count > baseline )); then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 0.1
  done
  return 1
}

wait_log_count_at_least() {
  local file="$1"
  local timeout_ms="$2"
  local minimum="$3"
  local pattern="$4"
  local deadline=$(( SECONDS + ((timeout_ms + 999) / 1000) ))
  local count
  while :; do
    count=$(grep -cE "${pattern}" "${file}" 2>/dev/null || true)
    if (( count >= minimum )); then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 0.1
  done
  return 1
}

send_signal() {
  local target="$1"
  local signal_name="$2"
  local target_pid
  target_pid=$(require_target_pid "${target}")
  if [[ "${target}" == "fd" ]]; then
    "${SUDO[@]}" kill "-${signal_name}" -- "-${target_pid}"
  else
    "${SUDO[@]}" kill "-${signal_name}" "${target_pid}"
  fi
}

log_operator_command() {
  local cmd="$1"
  shift
  printf 'operator: %s' "${cmd}"
  local arg
  for arg in "$@"; do
    printf ' %s' "${arg}"
  done
  printf '\n'
}

restart_fd() {
  [[ -n "${FD_LOG}" ]] || die "fd log path required for restart_fd"
  [[ -n "${FD_PID_FILE}" ]] || die "fd pid file required for restart_fd"

  local old_pid=""
  if old_pid=$(require_target_pid fd 2>/dev/null); then
    echo "operator: restart_fd stopping pid=${old_pid}"
    "${ROOT}/contrib/dump-live-validator-coverage.sh" \
      --pid "${old_pid}" \
      --profile-pattern "${VALIDATOR_PROFILE_PATTERN}" \
      --phase pre-restart || true
    "${SUDO[@]}" kill -TERM -- "-${old_pid}" 2>/dev/null || true
    if ! wait_pid_exit "${old_pid}" 30; then
      die "restart_fd: existing fd pid ${old_pid} did not exit"
    fi
  else
    echo "operator: restart_fd starting without a live existing pid"
  fi

  rm -f "${FD_PID_FILE}"
  local validator_env=()
  if [[ -n "${VALIDATOR_PROFILE_PATTERN}" ]]; then
    validator_env=( env "LLVM_PROFILE_FILE=${VALIDATOR_PROFILE_PATTERN}" )
  fi
  "${SUDO[@]}" "${validator_env[@]}" bash -lc '
    pid_file="$1"
    fddev_bin="$2"
    config_path="$3"
    log_file="$4"
    setsid "${fddev_bin}" dev --no-watch --no-configure --config "${config_path}" >> "${log_file}" 2>&1 &
    child_pid=$!
    echo "${child_pid}" > "${pid_file}"
    wait "${child_pid}"
  ' bash "${FD_PID_FILE}" "${FD_BIN}" "${CONFIG}" "${FD_LOG}" &
  local supervisor_pid=$!

  for _ in $(seq 1 50); do
    if pid=$(read_pid_file "${FD_PID_FILE}" 2>/dev/null); then
      FD_PID="${pid}"
      echo "operator: restart_fd started pid=${FD_PID} supervisor=${supervisor_pid}"
      return 0
    fi
    sleep 0.2
  done

  die "restart_fd: failed to capture restarted fd pid"
}

restart_bam() {
  [[ -n "${BAM_LOG}" ]] || die "bam log path required for restart_bam"
  [[ -n "${BAM_PID_FILE}" ]] || die "bam pid file required for restart_bam"
  [[ -n "${BAM_START_SCRIPT}" && -x "${BAM_START_SCRIPT}" ]] || die "bam start script required for restart_bam"

  local old_pid=""
  if old_pid=$(require_target_pid bam 2>/dev/null); then
    echo "operator: restart_bam stopping pid=${old_pid}"
    kill -TERM "${old_pid}" 2>/dev/null || true
    if ! wait_pid_exit "${old_pid}" 30; then
      die "restart_bam: existing bam pid ${old_pid} did not exit"
    fi
  else
    echo "operator: restart_bam starting without a live existing pid"
  fi

  rm -f "${BAM_PID_FILE}"
  "${BAM_START_SCRIPT}" >> "${BAM_LOG}" 2>&1 &
  local new_pid=$!
  write_pid_file "${BAM_PID_FILE}" "${new_pid}"
  BAM_PID="${new_pid}"
  echo "operator: restart_bam started pid=${BAM_PID}"
}

line_no=0
while IFS= read -r raw_line || [[ -n "${raw_line}" ]]; do
  line_no=$((line_no + 1))
  line="${raw_line%%#*}"
  if [[ -z "${line//[[:space:]]/}" ]]; then
    continue
  fi

  IFS=' ' read -r -a parts <<< "${line}"
  cmd="${parts[0]}"
  case "${cmd}" in
    sleep_ms)
      [[ "${#parts[@]}" -eq 2 ]] || die "${EVENTS}:${line_no}: sleep_ms expects exactly one argument"
      echo "operator: sleep_ms ${parts[1]}"
      sleep_ms "${parts[1]}"
      ;;
    wait_log)
      IFS=' ' read -r _ target timeout_ms pattern <<< "${line}"
      [[ -n "${target:-}" && -n "${timeout_ms:-}" && -n "${pattern:-}" ]] || die "${EVENTS}:${line_no}: wait_log expects target, timeout_ms, and regex"
      file=$(require_target_log "${target}")
      echo "operator: wait_log ${target} ${timeout_ms} ${pattern}"
      if ! wait_log_pattern "${file}" "${timeout_ms}" "${pattern}"; then
        echo "--- tail ${file} ---" >&2
        tail -n 80 "${file}" >&2 || true
        die "${EVENTS}:${line_no}: wait_log timed out for ${target} regex ${pattern}"
      fi
      ;;
    wait_log_growth)
      IFS=' ' read -r _ target timeout_ms pattern <<< "${line}"
      [[ -n "${target:-}" && -n "${timeout_ms:-}" && -n "${pattern:-}" ]] || die "${EVENTS}:${line_no}: wait_log_growth expects target, timeout_ms, and regex"
      file=$(require_target_log "${target}")
      echo "operator: wait_log_growth ${target} ${timeout_ms} ${pattern}"
      if ! wait_log_growth "${file}" "${timeout_ms}" "${pattern}"; then
        echo "--- tail ${file} ---" >&2
        tail -n 80 "${file}" >&2 || true
        die "${EVENTS}:${line_no}: wait_log_growth timed out for ${target} regex ${pattern}"
      fi
      ;;
    wait_log_count)
      IFS=' ' read -r _ target timeout_ms minimum pattern <<< "${line}"
      [[ -n "${target:-}" && -n "${timeout_ms:-}" && -n "${minimum:-}" && -n "${pattern:-}" ]] || die "${EVENTS}:${line_no}: wait_log_count expects target, timeout_ms, minimum, and regex"
      file=$(require_target_log "${target}")
      echo "operator: wait_log_count ${target} ${timeout_ms} ${minimum} ${pattern}"
      if ! wait_log_count_at_least "${file}" "${timeout_ms}" "${minimum}" "${pattern}"; then
        echo "--- tail ${file} ---" >&2
        tail -n 80 "${file}" >&2 || true
        die "${EVENTS}:${line_no}: wait_log_count timed out for ${target} regex ${pattern} minimum ${minimum}"
      fi
      ;;
    signal)
      [[ "${#parts[@]}" -eq 3 ]] || die "${EVENTS}:${line_no}: signal expects a target and signal name"
      echo "operator: signal ${parts[1]} ${parts[2]}"
      case "${parts[2],,}" in
        stop) send_signal "${parts[1]}" STOP ;;
        cont) send_signal "${parts[1]}" CONT ;;
        term) send_signal "${parts[1]}" TERM ;;
        kill) send_signal "${parts[1]}" KILL ;;
        int)  send_signal "${parts[1]}" INT  ;;
        *) die "${EVENTS}:${line_no}: unsupported signal ${parts[2]}" ;;
      esac
      ;;
    restart_fd)
      [[ "${#parts[@]}" -eq 1 ]] || die "${EVENTS}:${line_no}: restart_fd expects no arguments"
      restart_fd
      ;;
    restart_bam)
      [[ "${#parts[@]}" -eq 1 ]] || die "${EVENTS}:${line_no}: restart_bam expects no arguments"
      restart_bam
      ;;
    set_bam)
      [[ "${#parts[@]}" -ge 2 ]] || die "${EVENTS}:${line_no}: set_bam expects at least one flag"
      log_operator_command set_bam "${parts[@]:1}"
      "${SUDO[@]}" "${CONTROL_BIN}" set-bam --config "${CONFIG}" "${parts[@]:1}"
      ;;
    set_bam_expect_fail)
      [[ "${#parts[@]}" -ge 2 ]] || die "${EVENTS}:${line_no}: set_bam_expect_fail expects at least one flag"
      log_operator_command set_bam_expect_fail "${parts[@]:1}"
      if "${SUDO[@]}" "${CONTROL_BIN}" set-bam --config "${CONFIG}" "${parts[@]:1}"; then
        die "${EVENTS}:${line_no}: set_bam unexpectedly succeeded"
      fi
      ;;
    set_bam_clear_url)
      [[ "${#parts[@]}" -eq 1 ]] || die "${EVENTS}:${line_no}: set_bam_clear_url expects no arguments"
      echo "operator: set_bam_clear_url"
      "${SUDO[@]}" "${CONTROL_BIN}" set-bam --config "${CONFIG}" --url ""
      ;;
    set_bam_clear_sni)
      [[ "${#parts[@]}" -eq 1 ]] || die "${EVENTS}:${line_no}: set_bam_clear_sni expects no arguments"
      echo "operator: set_bam_clear_sni"
      "${SUDO[@]}" "${CONTROL_BIN}" set-bam --config "${CONFIG}" --sni ""
      ;;
    set_identity)
      [[ "${#parts[@]}" -ge 2 ]] || die "${EVENTS}:${line_no}: set_identity expects a keypair path"
      log_operator_command set_identity "${parts[@]:1}"
      "${SUDO[@]}" "${CONTROL_BIN}" set-identity --config "${CONFIG}" "${parts[@]:1}"
      ;;
    *)
      die "${EVENTS}:${line_no}: unsupported event ${cmd}"
      ;;
  esac
done < "${EVENTS}"
