#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

PID=""
PROFILE_PATTERN="${VALIDATOR_PROFILE_PATTERN:-}"
PHASE="snapshot"
USE_SUDO="${USE_SUDO:-1}"

while (($#)); do
  case "$1" in
    --pid)
      PID="$2"
      shift 2
      ;;
    --profile-pattern)
      PROFILE_PATTERN="$2"
      shift 2
      ;;
    --phase)
      PHASE="$2"
      shift 2
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

[[ "${PID}" =~ ^[0-9]+$ ]] || exit 0
[[ -n "${PROFILE_PATTERN}" ]] || exit 0
command -v gdb >/dev/null 2>&1 || {
  echo "warning: gdb is required to snapshot live validator coverage" >&2
  exit 0
}

SUDO=()
if [[ "${USE_SUDO}" != "0" && "$(id -u)" -ne 0 ]]; then
  SUDO=( sudo )
fi

profile_prefix="${PROFILE_PATTERN%%\%m*}"
mkdir -p "$(dirname -- "${profile_prefix}")"
phase_safe="${PHASE//[^a-zA-Z0-9_.-]/_}"

dump_process() {
  local process_pid="$1"
  local label="$2"
  local profile="${profile_prefix}${phase_safe}-${label}-${process_pid}.profraw"
  local escaped_profile="${profile//\\/\\\\}"
  escaped_profile="${escaped_profile//\"/\\\"}"
  local gdb_log="${profile%.profraw}.gdb.log"

  "${SUDO[@]}" gdb -q -nx -batch -p "${process_pid}" \
    -ex "call (void)__llvm_profile_set_filename(\"${escaped_profile}\")" \
    -ex 'call (int)__llvm_profile_write_file()' \
    -ex detach >"${gdb_log}" 2>&1 || true
  if [[ ! -s "${profile}" ]]; then
    echo "warning: failed to snapshot ${label} pid ${process_pid}; see ${gdb_log}" >&2
  fi
}

declare -A seen=()
if kill -0 "${PID}" 2>/dev/null; then
  dump_process "${PID}" parent
  seen["${PID}"]=1
fi

while IFS=' ' read -r process_pid args; do
  [[ -n "${process_pid}" && -n "${args}" ]] || continue
  [[ -z "${seen[${process_pid}]:-}" ]] || continue
  if [[ "${args}" =~ [[:space:]]run-agave([[:space:]]|$) ]]; then
    dump_process "${process_pid}" agave
  elif [[ "${args}" =~ [[:space:]]run1[[:space:]]+([^[:space:]]+)[[:space:]]+([0-9]+)([[:space:]]|$) ]]; then
    dump_process "${process_pid}" "${BASH_REMATCH[1]}-${BASH_REMATCH[2]}"
  else
    continue
  fi
  seen["${process_pid}"]=1
done < <(ps -o pid=,args= -g "${PID}")
