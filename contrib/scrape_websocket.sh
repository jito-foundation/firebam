#!/usr/bin/env bash
# Scrape Firedancer GUI websocket data and emit per-slot TPU Waterfall values.
# Flow:
# 1) Capture a websocket snapshot to discover validator identity and produced slots.
# 2) Query `slot.query_detailed` for those produced slots (recent or since-startup).
# 3) Compute Sankey node/drop values and slot metrics from waterfall counters.
# Output: NDJSON (one JSON object per slot).
set -euo pipefail

DEFAULT_HOST="64.130.59.250"
DEFAULT_MODE="recent"
DEFAULT_RECENT_COUNT="16"
DEFAULT_SNAPSHOT_SECS="2"
DEFAULT_QUERY_WAIT_SECS="8"
DEFAULT_DETAIL_TIMEOUT_SECS="60"
DEFAULT_OUTPUT_FILE=""

HOST="${DEFAULT_HOST}"
MODE="${DEFAULT_MODE}"
RECENT_COUNT="${DEFAULT_RECENT_COUNT}"
SNAPSHOT_SECS="${DEFAULT_SNAPSHOT_SECS}"
QUERY_WAIT_SECS="${DEFAULT_QUERY_WAIT_SECS}"
DETAIL_TIMEOUT_SECS="${DEFAULT_DETAIL_TIMEOUT_SECS}"
OUTPUT_FILE="${DEFAULT_OUTPUT_FILE}"

usage() {
  cat <<EOF
Usage:
  scrape_websocket.sh [OPTIONS]

Options:
  --host HOST                      Websocket host (default: ${DEFAULT_HOST})
  --mode MODE                      One of: recent, since-startup (default: ${DEFAULT_MODE})
  --recent-count N                 Slots to keep in recent mode (default: ${DEFAULT_RECENT_COUNT})
  --snapshot-secs N                Max seconds to wait for required snapshot messages (default: ${DEFAULT_SNAPSHOT_SECS})
  --query-wait-secs N              Idle seconds without new query responses before retrying (default: ${DEFAULT_QUERY_WAIT_SECS})
  --detail-timeout-secs N          Max seconds per detailed query attempt (default: ${DEFAULT_DETAIL_TIMEOUT_SECS})
  --output-file PATH               Write NDJSON results to PATH instead of stdout
  -h, --help                       Show this help

Modes:
  recent         Return the newest RECENT_COUNT produced slots.
  since-startup  Return produced slots completed since validator startup time.

Notes:
  - RECENT_COUNT is only used when MODE=recent.
  - Output is NDJSON (one JSON object per slot).

Examples:
  # Newest 16 produced slots from host 64.130.59.250
  scrape_websocket.sh --host 64.130.59.250 --mode recent --recent-count 16

  # All produced slots completed since startup for host 64.130.59.250
  scrape_websocket.sh --host 64.130.59.250 --mode since-startup

  # More aggressive timing and larger recent window
  scrape_websocket.sh --host localhost --mode recent --recent-count 50 --snapshot-secs 3 --query-wait-secs 6 --detail-timeout-secs 30
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      [[ $# -ge 2 ]] || { echo "error: --host requires a value" >&2; usage >&2; exit 1; }
      HOST="$2"
      shift 2
      ;;
    --host=*)
      HOST="${1#*=}"
      shift
      ;;
    --mode)
      [[ $# -ge 2 ]] || { echo "error: --mode requires a value" >&2; usage >&2; exit 1; }
      MODE="$2"
      shift 2
      ;;
    --mode=*)
      MODE="${1#*=}"
      shift
      ;;
    --recent-count)
      [[ $# -ge 2 ]] || { echo "error: --recent-count requires a value" >&2; usage >&2; exit 1; }
      RECENT_COUNT="$2"
      shift 2
      ;;
    --recent-count=*)
      RECENT_COUNT="${1#*=}"
      shift
      ;;
    --snapshot-secs)
      [[ $# -ge 2 ]] || { echo "error: --snapshot-secs requires a value" >&2; usage >&2; exit 1; }
      SNAPSHOT_SECS="$2"
      shift 2
      ;;
    --snapshot-secs=*)
      SNAPSHOT_SECS="${1#*=}"
      shift
      ;;
    --query-wait-secs)
      [[ $# -ge 2 ]] || { echo "error: --query-wait-secs requires a value" >&2; usage >&2; exit 1; }
      QUERY_WAIT_SECS="$2"
      shift 2
      ;;
    --query-wait-secs=*)
      QUERY_WAIT_SECS="${1#*=}"
      shift
      ;;
    --detail-timeout-secs)
      [[ $# -ge 2 ]] || { echo "error: --detail-timeout-secs requires a value" >&2; usage >&2; exit 1; }
      DETAIL_TIMEOUT_SECS="$2"
      shift 2
      ;;
    --detail-timeout-secs=*)
      DETAIL_TIMEOUT_SECS="${1#*=}"
      shift
      ;;
    --output-file)
      [[ $# -ge 2 ]] || { echo "error: --output-file requires a value" >&2; usage >&2; exit 1; }
      OUTPUT_FILE="$2"
      shift 2
      ;;
    --output-file=*)
      OUTPUT_FILE="${1#*=}"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      [[ $# -eq 0 ]] || { echo "error: positional args are not supported; use long options only" >&2; usage >&2; exit 1; }
      ;;
    -*)
      echo "error: unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      echo "error: positional args are not supported; use long options only (unexpected: $1)" >&2
      usage >&2
      exit 1
      ;;
  esac
done

for bin in websocat jq; do
  command -v "${bin}" >/dev/null 2>&1 || { echo "error: ${bin} not found in PATH" >&2; exit 1; }
done
[[ -n "${HOST}" ]] || { echo "error: --host must be non-empty" >&2; usage >&2; exit 1; }
[[ "${MODE}" =~ ^(recent|since-startup)$ ]] || { echo "error: MODE must be 'recent' or 'since-startup'" >&2; usage >&2; exit 1; }
for n in "${RECENT_COUNT}" "${SNAPSHOT_SECS}" "${QUERY_WAIT_SECS}" "${DETAIL_TIMEOUT_SECS}"; do
  [[ "${n}" =~ ^[0-9]+$ ]] || { echo "error: numeric args must be integers >= 0" >&2; exit 1; }
done

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

snapshot_file="${tmpdir}/snapshot.ndjson"
snapshot_meta_file="${tmpdir}/snapshot_meta.json"
query_file="${tmpdir}/query.ndjson"
details_file="${tmpdir}/details.ndjson"
results_out_file="${tmpdir}/results_out.ndjson"

capture_snapshot_until_ready() {
  local out_file="$1"
  local max_wait_secs="$2"

  local got_identity=0
  local got_startup=0
  local got_completed=0
  local got_covering_epoch=0

  : > "${out_file}"

  coproc SNAPSHOT_WS { websocat -B 12000000 "ws://${HOST}:80/websocket" 2>/dev/null; }
  local ws_pid="${SNAPSHOT_WS_PID:-}"
  local ws_out_fd="${SNAPSHOT_WS[0]:-}"
  local ws_in_fd="${SNAPSHOT_WS[1]:-}"
  if [[ -z "${ws_out_fd}" || -z "${ws_in_fd}" ]]; then
    if [[ -n "${ws_pid}" ]]; then
      wait "${ws_pid}" 2>/dev/null || true
    fi
    return 1
  fi
  local deadline=$((SECONDS + max_wait_secs))
  local line=""
  local json_line=""
  local msg_kind=""
  local completed_slot=""

  while (( SECONDS <= deadline )); do
    if IFS= read -r -u "${ws_out_fd}" -t 1 line 2>/dev/null; then
      json_line="$(jq -c 'if type=="object" then . else empty end' <<< "${line}" 2>/dev/null || true)"
      [[ -n "${json_line}" ]] || continue
      printf '%s\n' "${json_line}" >> "${out_file}"

      msg_kind="$(jq -r '
        if .topic=="summary" and .key=="identity_key" and .value!=null then "identity"
        elif .topic=="summary" and .key=="startup_time_nanos" and .value!=null then "startup"
        elif .topic=="summary" and .key=="completed_slot" and .value!=null then "completed"
        elif .topic=="epoch" and .key=="new" then "epoch"
        else empty
        end
      ' <<< "${json_line}" 2>/dev/null || true)"

      case "${msg_kind}" in
        identity) got_identity=1 ;;
        startup) got_startup=1 ;;
        completed)
          got_completed=1
          completed_slot="$(jq -r '
            if .topic=="summary" and .key=="completed_slot" and .value!=null
            then (.value | tostring)
            else empty
            end
          ' <<< "${json_line}" 2>/dev/null || true)"
          ;;
      esac

      if (( got_identity && got_startup && got_completed && got_covering_epoch == 0 )) && [[ -n "${completed_slot}" ]]; then
        if jq -e -s --argjson completed "${completed_slot}" '
          any(
            .[];
            .topic=="epoch"
            and .key=="new"
            and (.value | type)=="object"
            and ((.value.start_slot // 0) <= $completed)
            and ((.value.end_slot // -1) >= $completed)
          )
        ' "${out_file}" >/dev/null 2>&1; then
          got_covering_epoch=1
        fi
      fi

      if (( got_identity && got_startup && got_completed && got_covering_epoch )); then
        break
      fi
    fi
  done

  [[ -n "${ws_in_fd}" ]] && exec {ws_in_fd}>&- || true
  [[ -n "${ws_out_fd}" ]] && exec {ws_out_fd}<&- || true
  if [[ -n "${ws_pid}" ]]; then
    kill "${ws_pid}" 2>/dev/null || true
    wait "${ws_pid}" 2>/dev/null || true
  fi

  (( got_identity && got_startup && got_completed && got_covering_epoch ))
}

collect_query_details() {
  local request_file="$1"
  local out_file="$2"
  local idle_wait_secs="$3"
  local max_wait_secs="$4"

  local -A pending_ids=()
  local -A seen_ids=()
  local request_id=""
  while IFS= read -r request_id; do
    [[ -n "${request_id}" ]] || continue
    pending_ids["${request_id}"]=1
  done < <(jq -r '.id // empty | tostring' "${request_file}")

  local expected_count="${#pending_ids[@]}"
  : > "${out_file}"
  (( expected_count > 0 )) || return 0

  coproc DETAIL_WS { websocat -B 12000000 "ws://${HOST}:80/websocket" 2>/dev/null; }
  local ws_pid="${DETAIL_WS_PID:-}"
  local ws_out_fd="${DETAIL_WS[0]:-}"
  local ws_in_fd="${DETAIL_WS[1]:-}"
  if [[ -z "${ws_out_fd}" || -z "${ws_in_fd}" ]]; then
    if [[ -n "${ws_pid}" ]]; then
      wait "${ws_pid}" 2>/dev/null || true
    fi
    return 1
  fi
  local send_failed=0
  local query_line=""
  while IFS= read -r query_line; do
    [[ -n "${query_line}" ]] || continue
    if ! printf '%s\n' "${query_line}" >&"${ws_in_fd}"; then
      send_failed=1
      break
    fi
  done < "${request_file}"

  local deadline=$((SECONDS + max_wait_secs))
  local idle_deadline=$((SECONDS + idle_wait_secs))
  local line=""
  local json_line=""
  while (( SECONDS <= deadline )); do
    (( send_failed == 0 )) || break
    if (( ${#seen_ids[@]} >= expected_count )); then
      break
    fi

    if IFS= read -r -u "${ws_out_fd}" -t 1 line 2>/dev/null; then
      json_line="$(jq -c 'if type=="object" then . else empty end' <<< "${line}" 2>/dev/null || true)"
      [[ -n "${json_line}" ]] || continue
      printf '%s\n' "${json_line}" >> "${out_file}"
      if [[ "${json_line}" == *'"topic":"slot"'* ]] && [[ "${json_line}" == *'"id":'* ]] && \
         ( [[ "${json_line}" == *'"key":"query"'* ]] || [[ "${json_line}" == *'"key":"query_detailed"'* ]] ); then
        local response_id=""
        response_id="$(jq -r '
          if .topic=="slot" and ((.key=="query") or (.key=="query_detailed")) and (.id!=null)
          then (.id|tostring)
          else empty
          end
        ' <<< "${json_line}" 2>/dev/null || true)"
        if [[ -n "${response_id}" ]] && [[ -n "${pending_ids[${response_id}]+x}" ]] && [[ -z "${seen_ids[${response_id}]+x}" ]]; then
          seen_ids["${response_id}"]=1
          idle_deadline=$((SECONDS + idle_wait_secs))
        fi
      fi
    fi

    if (( SECONDS > idle_deadline )) && (( ${#seen_ids[@]} < expected_count )); then
      break
    fi
  done

  [[ -n "${ws_in_fd}" ]] && exec {ws_in_fd}>&- || true
  [[ -n "${ws_out_fd}" ]] && exec {ws_out_fd}<&- || true
  if [[ -n "${ws_pid}" ]]; then
    kill "${ws_pid}" 2>/dev/null || true
    wait "${ws_pid}" 2>/dev/null || true
  fi

  (( send_failed == 0 )) || return 1
  (( ${#seen_ids[@]} >= expected_count ))
}

if ! capture_snapshot_until_ready "${snapshot_file}" "${SNAPSHOT_SECS}"; then
  echo "error: timed out waiting for required websocket summary data and an epoch schedule covering completed_slot; try increasing SNAPSHOT_SECS" >&2
  exit 1
fi
[[ -s "${snapshot_file}" ]] || { echo "error: websocket snapshot is empty" >&2; exit 1; }

if ! jq -sc '
  def produced_slots($identity; $completed):
    [
      .[]
      | select(.topic=="epoch" and .key=="new")
      | .value as $e
      | ($e.staked_pubkeys | index($identity)) as $idx
      | select($idx != null)
      | $e.leader_slots
      | to_entries[]
      | select(.value == $idx)
      | ($e.start_slot + (.key * 4) + 0),
        ($e.start_slot + (.key * 4) + 1),
        ($e.start_slot + (.key * 4) + 2),
        ($e.start_slot + (.key * 4) + 3)
      | select(. <= $completed)
    ]
    | sort
    | unique;

  (first(.[] | select(.topic=="summary" and .key=="identity_key" and .value!=null) | .value)) as $identity
  | (first(.[] | select(.topic=="summary" and .key=="startup_time_nanos" and .value!=null) | .value)) as $startup_ns
  | (first(.[] | select(.topic=="summary" and .key=="completed_slot" and .value!=null) | .value)) as $completed
  | [ .[]
      | select(.topic=="epoch" and .key=="new" and (.value | type) == "object")
      | {
          start_slot: (.value.start_slot // 0),
          end_slot: (.value.end_slot // -1)
        }
    ] as $epoch_ranges
  | if ($identity == null or $startup_ns == null or $completed == null)
    then error("missing required summary fields in websocket snapshot")
    elif ([ $epoch_ranges[] | select(.start_slot <= $completed and .end_slot >= $completed) ] | length) == 0
    then error("missing websocket epoch schedule covering completed_slot")
    else {
      identity_key: $identity,
      startup_time_nanos: ($startup_ns | tostring),
      completed_slot: $completed,
      epoch_ranges: $epoch_ranges,
      produced_slots: produced_slots($identity; $completed)
    }
    end
' "${snapshot_file}" > "${snapshot_meta_file}"; then
  epoch_ranges="$(jq -sc '
    [ .[]
      | select(.topic=="epoch" and .key=="new" and (.value | type) == "object")
      | "\(.value.start_slot // 0)-\(.value.end_slot // -1)"
    ] | join(", ")
  ' "${snapshot_file}" 2>/dev/null || true)"
  [[ -n "${epoch_ranges}" && "${epoch_ranges}" != '""' ]] || epoch_ranges="none"
  echo "error: websocket snapshot missing required summary fields or an epoch schedule covering completed_slot; captured epoch ranges: ${epoch_ranges}" >&2
  exit 1
fi

startup_time_nanos="$(jq -r '.startup_time_nanos' "${snapshot_meta_file}")"
slots_count="$(jq -r '.produced_slots | length' "${snapshot_meta_file}")"
[[ "${slots_count}" -gt 0 ]] || { echo "error: no produced slots discovered from epoch schedules" >&2; exit 1; }

recent_candidate_count=0
if [[ "${MODE}" == "recent" && "${RECENT_COUNT}" -gt 0 ]]; then
  recent_candidate_count=$((RECENT_COUNT * 4))
  (( recent_candidate_count < RECENT_COUNT + 8 )) && recent_candidate_count=$((RECENT_COUNT + 8))
fi

jq -rc --arg mode "${MODE}" --argjson recent_candidate_count "${recent_candidate_count}" '
  .produced_slots
  | if $mode=="recent" and $recent_candidate_count > 0 and (length > $recent_candidate_count)
    then .[-$recent_candidate_count:]
    else .
    end
  | to_entries[]
  | {
      topic: "slot",
      key: "query_detailed",
      id: (1000 + .key),
      params: { slot: .value }
    }
' "${snapshot_meta_file}" > "${query_file}"

parse_results() {
  jq -nrc --arg mode "${MODE}" --arg startup_ns "${startup_time_nanos}" --argjson recent_count "${RECENT_COUNT}" '
    def z: . // 0;
    def sol4: ((./1000000000) * 10000 | round / 10000);
    def pos: if . > 0 then . else 0 end;
    def ge_big($x; $y):
      (($x|length) > ($y|length)) or ((($x|length)==($y|length)) and ($x >= $y));
    def parse_slot:
      if (.topic=="slot" and ((.key=="query") or (.key=="query_detailed")) and (.value != null))
      then
        .value as $v
        | $v.publish as $p
        | ($p.slot // null) as $slot
        | if $slot == null then null else
          ($v.waterfall.in // {}) as $i
          | ($v.waterfall.out // {}) as $o
          | ($i.pack_cranked|z) as $crank
          | ((($o.resolv_retained|z) - ($i.resolv_retained|z)) | pos) as $unresolved
          | (($i.quic|z) + ($i.udp|z)) as $received
          | ($received + ($i.gossip|z) - ($o.quic_abandoned|z)) as $verify
          | ($verify - ($o.verify_failed|z) - ($o.verify_duplicate|z)) as $dedup
          | ($dedup - ($o.dedup_duplicate|z)) as $resolv
          | ($resolv
             + $crank
             + ($i.pack_retained|z)
             - $unresolved
             - ($o.resolv_lut_failed|z)
             - ($o.resolv_expired|z)
             - ($o.resolv_ancient|z)
             - ($o.resolv_no_ledger|z)) as $pack
          | ($pack - ($o.pack_retained|z) - ($o.pack_invalid|z) - ($o.pack_expired|z) - ($o.pack_already_executed|z)) as $bank
          | ($bank - ($o.bank_invalid|z)) as $packed
          | {
              slot: $slot,
              sankey_nodes: {
                quic: ($i.quic|z),
                udp: ($i.udp|z),
                received: $received,
                gossip: ($i.gossip|z),
                verify: $verify,
                dedup: $dedup,
                resolv: $resolv,
                crank: $crank,
                buffered_in: ($i.pack_retained|z),
                pack: $pack,
                bank: $bank,
                packed: $packed
              },
              sankey_drops: {
                abandoned: ($o.quic_abandoned|z),
                bad_signature: ($o.verify_failed|z),
                verify_duplicate: ($o.verify_duplicate|z),
                dedup_duplicate: ($o.dedup_duplicate|z),
                unresolved: $unresolved,
                bad_lut: ($o.resolv_lut_failed|z),
                resolv_expired: ($o.resolv_expired|z),
                buffered: ($o.pack_retained|z),
                unpackable: ($o.pack_invalid|z),
                pack_expired: ($o.pack_expired|z),
                already_executed: ($o.pack_already_executed|z),
                unexecutable: ($o.bank_invalid|z),
                block_success: ($o.block_success|z),
                block_fail: ($o.block_fail|z)
              },
              slot_metrics: {
                votes: ($p.success_vote_transaction_cnt|z),
                non_vote_failure: ($p.failed_nonvote_transaction_cnt|z),
                non_vote_success: ($p.success_nonvote_transaction_cnt|z),
                compute_units: ($p.compute_units|z),
                priority_fees_sol: (($p.priority_fee|z) | sol4),
                transaction_fees_sol: (($p.transaction_fee|z) | sol4),
                tips_sol: (($p.tips|z) | sol4)
              },
              publish: $p
            }
        end
      else null
      end;

    reduce inputs as $row (
      {};
      ($row | parse_slot) as $parsed
      | if $parsed == null then . else .[($parsed.slot|tostring)] = $parsed end
    )
    | [ .[] ]
    | sort_by(.slot)
    | if $mode=="since-startup"
      then map(select(ge_big(((.publish.completed_time_nanos // "0")|tostring); $startup_ns)))
      else .
      end
    | if $mode=="recent" and $recent_count > 0 and (length > $recent_count)
      then .[-$recent_count:]
      else .
      end
    | .[]
  ' < "${details_file}" > "${results_out_file}"
}

seen_query_results=0
for attempt in 1 2; do
  wait_secs="${QUERY_WAIT_SECS}"
  timeout_secs="${DETAIL_TIMEOUT_SECS}"
  if [[ "${attempt}" == "2" ]]; then
    wait_secs=$((QUERY_WAIT_SECS + 4))
    timeout_secs=$((DETAIL_TIMEOUT_SECS + 40))
  fi

  collect_query_details "${query_file}" "${details_file}" "${wait_secs}" "${timeout_secs}" || true

  query_result_count="$(jq -sc '[.[] | select(.topic=="slot" and ((.key=="query") or (.key=="query_detailed")) and (.value != null))] | length' "${details_file}")"
  if [[ "${query_result_count}" -gt 0 ]]; then
    seen_query_results=1
  fi

  parse_results
  [[ -s "${results_out_file}" ]] && break
done

[[ "${seen_query_results}" -eq 1 ]] || { echo "error: no slot query results returned; try increasing QUERY_WAIT_SECS/DETAIL_TIMEOUT_SECS" >&2; exit 1; }
[[ -s "${results_out_file}" ]] || { echo "warning: no produced slots matched mode='${MODE}'" >&2; exit 0; }

if [[ -n "${OUTPUT_FILE}" ]]; then
  mkdir -p "$(dirname "${OUTPUT_FILE}")"
  cp "${results_out_file}" "${OUTPUT_FILE}"
else
  cat "${results_out_file}"
fi
