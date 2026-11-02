# shellcheck shell=bash
# Helpers shared by the local BAM harness scripts (fuzz-local-bam-stateful.sh,
# run-jito-agave-bam-scenario.sh, diff-local-bam-jito.sh and the
# validate-local-bam-* operator wrappers).  Sourced, not run; callers provide
# ROOT and, for the RPC/bridge helpers, RPC_URL, RPC_CURL_ARGS,
# CAPTURE_CHAIN_EVIDENCE, BRIDGE_BIN, FROM_GENESIS_ACCOUNT,
# GEN_SIMPLE_SYSTEM_TXNCTX_BIN and WRITE_SEEDED_KEYPAIR_BIN.

die() {
  echo "error: $*" >&2
  exit 1
}

regex_escape() {
  printf '%s' "$1" | sed -e 's/[][(){}.^$*+?|\\/]/\\&/g'
}

choose_free_tcp_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

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

rpc_account_info() {
  local pubkey="$1"
  local commitment="${2:-processed}"
  curl "${RPC_CURL_ARGS[@]}" "${RPC_URL}" \
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

rpc_send_base64_transaction() {
  local encoded="$1"
  curl "${RPC_CURL_ARGS[@]}" "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"sendTransaction\",\"params\":[\"${encoded}\",{\"encoding\":\"base64\",\"skipPreflight\":true,\"maxRetries\":20}]}" \
    | jq -e -r '.result'
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

wait_rpc_slot_at_least() { # minimum timeout_secs [slot_fn, default rpc_slot]
  local minimum="$1"
  local timeout_secs="$2"
  local slot_fn="${3:-rpc_slot}"
  local deadline=$((SECONDS + timeout_secs))
  local observed=0
  while (( SECONDS < deadline )); do
    observed=$("${slot_fn}" 2>/dev/null || printf '0')
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

metadata_get() {
  local metadata_file="$1"
  local key="$2"
  awk -F= -v key="${key}" '$1 == key { print substr($0, length(key) + 2); exit }' "${metadata_file}"
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

generate_raw_transfer_packet() { # txnctx packet gen_out bridge_out blockhash to_seed lamports bogus_lookup [alt] [cu_limit] [cu_price]
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
    --from-seed-index "${FROM_GENESIS_ACCOUNT}"
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

scenario_has_send_events() {
  local scenario_file="$1"
  grep -Eq 'type[[:space:]]*=[[:space:]]*"send_(batch|batch_flood|multi_batch|split_batch)"' "${scenario_file}" 2>/dev/null
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

write_max_schedule_slot_toml() {
  local value="$1"
  if [[ "${value}" =~ ^[0-9]+$ ]]; then
    printf 'max_schedule_slot = %s\n' "${value}"
  else
    printf 'max_schedule_slot = "%s"\n' "${value}"
  fi
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

append_random_mixed_batches() { # pattern packet_one packet_two seq_base: one scn_sub per token
  local pattern="$1"
  local packet_one="$2"
  local packet_two="$3"
  local seq_base="$4"

  local token seq_id idx=0
  local -a tokens=()
  local IFS=,
  read -r -a tokens <<< "${pattern}"
  for token in "${tokens[@]}"; do
    seq_id=$((seq_base + idx))
    case "${token}" in
      valid1|replay1) scn_sub "${seq_id}" "${packet_one}" ;;
      valid2|replay2) scn_sub "${seq_id}" "${packet_two}" ;;
      stale1)         scn_sub "${seq_id}" "${packet_one}" 0 ;;
      stale2)         scn_sub "${seq_id}" "${packet_two}" 0 ;;
      empty)          scn_sub "${seq_id}" 0 ;;
      *)
        die "unsupported random mixed token ${token}"
        ;;
    esac
    idx=$((idx + 1))
  done
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

# bam-test-server scripted scenario writers: scn_head starts SCN_FILE and the
# other scn_* append one event each.  For scn_send/scn_sub the packet argument
# is a packets_base64_file path or a bare packet_count, and max_schedule_slot is
# raw TOML (default "max").
scn_head() { # path comment description [top-level "key = value" ...]
  SCN_FILE="$1"
  printf '# %s\ndescription = "%s"\nheartbeat_interval_ms = 1000\n' "$2" "$3" >"${SCN_FILE}"
  shift 3
  (( $# == 0 )) || printf '%s\n' "$@" >>"${SCN_FILE}"
}

scn_event() { printf '\n[[events]]\ntype = "%s"\n' "$1" >>"${SCN_FILE}"; }

scn_batch() { # seq_id packets [max_schedule_slot] [revert_on_error] [simple_vote_tx]
  local src="packets_base64_file = \"$2\""
  [[ ! "$2" =~ ^[0-9]+$ ]] || src="packet_count = $2"
  printf 'seq_id = %s\n%s\nmax_schedule_slot = %s\nrevert_on_error = %s\nsimple_vote_tx = %s\n' \
    "$1" "${src}" "${3:-\"max\"}" "${4:-true}" "${5:-false}" >>"${SCN_FILE}"
}

scn_send()  { scn_event send_batch; scn_batch "$@"; }
scn_multi() { scn_event send_multi_batch; }
scn_sub()   { printf '\n[[events.batches]]\n' >>"${SCN_FILE}"; scn_batch "$@"; }
scn_sleep() { scn_event sleep; printf 'ms = %s\n' "$1" >>"${SCN_FILE}"; }

scn_queue_send() { # seq_id packets max_schedule_slot: queue bursts write the slot last
  scn_event send_batch
  printf 'seq_id = %s\npackets_base64_file = "%s"\nrevert_on_error = true\nsimple_vote_tx = false\n\n' "$1" "$2" >>"${SCN_FILE}"
  write_max_schedule_slot_toml "$3" >>"${SCN_FILE}"
}

scn_split() { # seq_id splits packets
  scn_event send_split_batch
  printf 'seq_id = %s\nsplits = %s\npackets_base64_file = "%s"\nmax_schedule_slot = "max"\nrevert_on_error = true\nsimple_vote_tx = false\n' \
    "$1" "$2" "$3" >>"${SCN_FILE}"
}
scn_close() { scn_event close_stream; }

scn_wait() { # kind min_count timeout_ms
  scn_event wait_inbound
  printf 'kind = "%s"\nmin_count = %s\ntimeout_ms = %s\n' "$1" "$2" "$3" >>"${SCN_FILE}"
}

scn_config_update() { # prio_fee_recipient_pubkey commission_bps
  scn_event config_update
  printf 'prio_fee_recipient_pubkey = "%s"\ncommission_bps = %s\n' "$1" "$2" >>"${SCN_FILE}"
}

# Shared body of the validate-local-bam-{fd,controller}-{restart,pause-resume}
# and url-sni-churn wrappers: configure the local dev node, then exec
# validate-local-bam-live.sh with the operator events file, the caller's
# LIVE_EXPECT expectations and any extra wrapper arguments.
run_live_operator_validation() { # events-file default-timeout-secs [wrapper args...]
  local events="$1"
  local default_timeout_secs="$2"
  shift 2
  local config="${ROOT}/contrib/local-bam.toml"
  local fd_bin="${FD_BIN:-${FDDEV:-${ROOT}/build/native/gcc/bin/firedancer-dev}}"
  local control_bin="${CONTROL_BIN:-${FDCTL:-${ROOT}/build/native/gcc/bin/firedancer-dev}}"
  local timeout_secs="${default_timeout_secs}"
  local -a passthru=()
  while (($#)); do
    case "$1" in
      --config)
        config="$2"
        shift 2
        ;;
      --fd-bin)
        fd_bin="$2"
        shift 2
        ;;
      --control-bin)
        control_bin="$2"
        shift 2
        ;;
      --timeout-secs)
        timeout_secs="$2"
        shift 2
        ;;
      --)
        shift
        passthru+=( "$@" )
        break
        ;;
      -h|--help)
        cat <<EOF2
Usage: $(basename "$0") [OPTIONS] [-- extra validate-local-bam-live args]

Options:
  --config PATH        Validator config path (default: contrib/local-bam.toml)
  --fd-bin PATH        Validator binary (default: build/native/gcc/bin/firedancer-dev)
  --control-bin PATH   Control binary for operator events (default: build/native/gcc/bin/firedancer-dev)
  --timeout-secs N     Validation timeout in seconds (default: ${default_timeout_secs})
  -h, --help           Show this help
EOF2
        exit 0
        ;;
      *)
        passthru+=( "$1" )
        shift
        ;;
    esac
  done

  [[ -x "${fd_bin}" ]] || die "missing validator binary ${fd_bin}"
  [[ -x "${control_bin}" ]] || die "missing control binary ${control_bin}"
  [[ -f "${config}" ]] || die "missing config ${config}"

  local -a sudo_cmd=()
  if [[ "${USE_SUDO:-1}" != "0" && "$(id -u)" -ne 0 ]]; then
    command -v sudo >/dev/null 2>&1 || die "sudo not found"
    sudo_cmd=( sudo )
  fi

  local configure_log
  configure_log=$(mktemp /tmp/firebam-configure.XXXXXX)
  "${sudo_cmd[@]}" env FD_DEV_PRESEED_BAM_TIP_PROGRAMS_DIR="${BAM_TIP_PROGRAMS_DIR:-${JITO_SOLANA_DIR:-${HOME}/jito-solana}/program-binaries/src/programs}" "${fd_bin}" configure init all --config "${config}" >"${configure_log}" 2>&1 \
    || die "configure init failed; see ${configure_log}"

  exec "${ROOT}/contrib/validate-local-bam-live.sh" \
    --scenario benign \
    --operator-events "${ROOT}/contrib/bam-operator-events/${events}" \
    --config "${config}" \
    --fd-bin "${fd_bin}" \
    --control-bin "${control_bin}" \
    --skip-configure \
    --timeout-secs "${timeout_secs}" \
    "${LIVE_EXPECT[@]}" \
    "${passthru[@]}"
}
