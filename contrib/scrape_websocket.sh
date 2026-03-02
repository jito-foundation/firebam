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
DEFAULT_QUERY_WAIT_SECS="4"
DEFAULT_DETAIL_TIMEOUT_SECS="25"

HOST="${DEFAULT_HOST}"
MODE="${DEFAULT_MODE}"
RECENT_COUNT="${DEFAULT_RECENT_COUNT}"
SNAPSHOT_SECS="${DEFAULT_SNAPSHOT_SECS}"
QUERY_WAIT_SECS="${DEFAULT_QUERY_WAIT_SECS}"
DETAIL_TIMEOUT_SECS="${DEFAULT_DETAIL_TIMEOUT_SECS}"

usage() {
  cat <<EOF
Usage:
  scrape_websocket.sh [OPTIONS]

Options:
  --host HOST                      Websocket host (default: ${DEFAULT_HOST})
  --mode MODE                      One of: recent, since-startup (default: ${DEFAULT_MODE})
  --recent-count N                 Slots to keep in recent mode (default: ${DEFAULT_RECENT_COUNT})
  --snapshot-secs N                Snapshot capture duration (default: ${DEFAULT_SNAPSHOT_SECS})
  --query-wait-secs N              Wait after sending queries (default: ${DEFAULT_QUERY_WAIT_SECS})
  --detail-timeout-secs N          Timeout for detailed query websocket (default: ${DEFAULT_DETAIL_TIMEOUT_SECS})
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

(sleep "${SNAPSHOT_SECS}") | timeout "$((SNAPSHOT_SECS + 5))s" websocat -B 12000000 "ws://${HOST}:80/websocket" > "${snapshot_file}" 2>/dev/null || true
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
  | if ($identity == null or $startup_ns == null or $completed == null)
    then error("missing required summary fields in websocket snapshot")
    else {
      identity_key: $identity,
      startup_time_nanos: ($startup_ns | tostring),
      completed_slot: $completed,
      produced_slots: produced_slots($identity; $completed)
    }
    end
' "${snapshot_file}" > "${snapshot_meta_file}"; then
  echo "error: missing required summary fields in websocket snapshot" >&2
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
    wait_secs=$((QUERY_WAIT_SECS + 2))
    timeout_secs=$((DETAIL_TIMEOUT_SECS + 20))
  fi
  (
    cat "${query_file}"
    sleep "${wait_secs}"
  ) | timeout "${timeout_secs}s" websocat -B 12000000 "ws://${HOST}:80/websocket" > "${details_file}" 2>/dev/null || true

  query_result_count="$(jq -rc '[select(.topic=="slot" and ((.key=="query") or (.key=="query_detailed")) and (.value != null))] | length' "${details_file}")"
  if [[ "${query_result_count}" -gt 0 ]]; then
    seen_query_results=1
  fi

  parse_results
  [[ -s "${results_out_file}" ]] && break
done

[[ "${seen_query_results}" -eq 1 ]] || { echo "error: no slot query results returned; try increasing QUERY_WAIT_SECS/DETAIL_TIMEOUT_SECS" >&2; exit 1; }
[[ -s "${results_out_file}" ]] || { echo "warning: no produced slots matched mode='${MODE}'" >&2; exit 0; }

cat "${results_out_file}"
