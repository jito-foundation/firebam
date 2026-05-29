#!/usr/bin/env bash
set -euo pipefail

# swap-identity.sh
#
# Swap the identity for a single validator type (Agave or Firedancer).
# - Toggles between staked and dummy identities based on IDENTITY_LINK_PATH.
# - Updates /etc/solana/identity.json symlink for restart consistency.
#
# This script does not stop or start systemd services; it only performs
# set-identity and file operations. Use --dry-run to inspect commands.

# ---- helpers ----
usage() {
  cat <<'USAGE'
Usage: swap-identity.sh agave|firedancer [--dry-run] [--wait]

Modes:
  agave       Swap identity for the Agave validator
  firedancer  Swap identity for the Firedancer validator

Behavior:
  If IDENTITY_LINK_PATH points at the staked identity, demote to the dummy
  identity. If it points at the dummy identity, promote to the staked
  identity.

  --wait  Wait for a restart window of $MIN_IDLE_TIME minutes before demotion
          (Agave mode only)
  --dry-run still generates the dummy identity if missing

Firedancer:
  Firedancer mode reads the running binary and --config path from /proc.

Environment overrides (defaults shown):
  AGAVE_DIR=${HOME}/jito-solana/docker-output
  AGAVE_BIN_PATH=${AGAVE_DIR}/agave-validator
  SOLANA_KEYGEN_PATH=${AGAVE_DIR}/solana-keygen
  AGAVE_LEDGER_PATH=/solana/ledger
  IDENTITY_LINK_PATH=/etc/solana/identity.json
  STAKED_IDENTITY_KEYPAIR_PATH=/etc/solana/staked-identity.json
  DUMMY_IDENTITY_KEYPAIR_PATH=/etc/solana/unstaked-identity.json
  MIN_IDLE_TIME=2       # minimum idle minutes for wait-for-restart-window
  SET_IDENTITY_FORCE=0  # set to 1 to add --force to set-identity
  ALLOW_NON_SYMLINK=0   # set to 1 to overwrite a non-symlink identity.json
  DUMMY_KEY_OWNER=      # optional owner for newly generated dummy key
  CLUSTER_RPC_URL=https://api.mainnet.solana.com
  SKIP_CLUSTER_CHECK=0  # set to 1 to skip checking target identity + interface is already in use
  DELINQUENT_SLOT_DISTANCE=4 # allow staked promote when slot lag is greater than this value
  CLUSTER_POLL_INTERVAL_SECS=1 # seconds between cluster polls while waiting to promote staked identity
USAGE
}

# Echo the command and execute it unless --dry-run is set.
run() {
  echo "+ $*"
  if [[ "$DRY_RUN" -eq 0 ]]; then
    "$@"
  fi
}

# ---- main ----
MODE=""
DRY_RUN=0
WAIT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    agave|firedancer) MODE="$1"; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --wait) WAIT=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done
[[ -n "$MODE" ]] || { usage; exit 2; }

AGAVE_DIR="${AGAVE_DIR:-${HOME}/jito-solana/docker-output}"
AGAVE_BIN_PATH="${AGAVE_BIN_PATH:-${AGAVE_DIR}/agave-validator}"
SOLANA_KEYGEN_PATH="${SOLANA_KEYGEN_PATH:-${AGAVE_DIR}/solana-keygen}"
AGAVE_LEDGER_PATH="${AGAVE_LEDGER_PATH:-/solana/ledger}"
IDENTITY_LINK_PATH="${IDENTITY_LINK_PATH:-/etc/solana/identity.json}"
STAKED_IDENTITY_KEYPAIR_PATH="${STAKED_IDENTITY_KEYPAIR_PATH:-/etc/solana/staked-identity.json}"
DUMMY_IDENTITY_KEYPAIR_PATH="${DUMMY_IDENTITY_KEYPAIR_PATH:-/etc/solana/unstaked-identity.json}"
MIN_IDLE_TIME="${MIN_IDLE_TIME:-2}"
SET_IDENTITY_FORCE="${SET_IDENTITY_FORCE:-0}"
ALLOW_NON_SYMLINK="${ALLOW_NON_SYMLINK:-0}"
CLUSTER_RPC_URL="${CLUSTER_RPC_URL:-https://api.mainnet.solana.com}"
SKIP_CLUSTER_CHECK="${SKIP_CLUSTER_CHECK:-0}"
DELINQUENT_SLOT_DISTANCE="${DELINQUENT_SLOT_DISTANCE:-4}"
CLUSTER_POLL_INTERVAL_SECS="${CLUSTER_POLL_INTERVAL_SECS:-1}"
FDCTL_BIN_PATH=""
FD_CONFIG_PATH=""

if [[ "$MODE" == "firedancer" ]]; then
  fd_pid= fd_key=
  for proc_dir in /proc/[0-9]*; do
    [[ -r "$proc_dir/cmdline" && -L "$proc_dir/exe" ]] || continue
    mapfile -d '' -t argv < "$proc_dir/cmdline" || continue
    [[ "${argv[0]-}" ]] || continue
    case "${argv[0]##*/}" in
      fdctl|fddev|firedancer|firedancer-dev) ;;
      *) continue ;;
    esac
    raw_config=
    for ((i=1; i<${#argv[@]}; i++)); do
      case "${argv[i]}" in
        --config) raw_config="${argv[i+1]-}"; break ;;
        --config=*) raw_config="${argv[i]#--config=}"; break ;;
      esac
    done
    [[ "$raw_config" ]] || continue
    exe_path="$(readlink -f -- "$proc_dir/exe" 2>/dev/null)" || continue
    pid="${proc_dir#/proc/}"
    parent_pid="$(awk '/^PPid:/ {print $2}' "$proc_dir/status" 2>/dev/null || true)"
    parent_exe="$(readlink -f -- "/proc/$parent_pid/exe" 2>/dev/null || true)"
    [[ "$parent_exe" != "$exe_path" ]] || continue
    cwd_path="$(readlink -f -- "$proc_dir/cwd" 2>/dev/null)" || continue
    resolved_path="$raw_config"
    [[ "$resolved_path" != /* ]] && resolved_path="$cwd_path/$resolved_path"
    resolved_config="$(readlink -f -- "$resolved_path" 2>/dev/null || printf '%s\n' "$resolved_path")"
    new_key="$exe_path|$resolved_config"
    if [[ -z "$fd_key" ]]; then
      fd_key="$new_key"; FDCTL_BIN_PATH="$exe_path"; FD_CONFIG_PATH="$resolved_config"; fd_pid="$pid"
    elif [[ "$fd_key" != "$new_key" ]]; then
      echo "error: multiple Firedancer instances with different configs detected" >&2
      echo "  candidate 1: pid $fd_pid -> $FDCTL_BIN_PATH --config $FD_CONFIG_PATH" >&2
      echo "  candidate 2: pid $pid -> $exe_path --config $resolved_config" >&2
      exit 1
    fi
  done
  if [[ -z "$FDCTL_BIN_PATH" || -z "$FD_CONFIG_PATH" ]]; then
    echo "error: could not detect a running Firedancer process with --config in /proc" >&2
    exit 1
  fi
  echo "info: detected Firedancer instance pid $fd_pid -> $FDCTL_BIN_PATH --config $FD_CONFIG_PATH"
fi

DEFAULT_OWNER="$(id -un)"
[[ "$DEFAULT_OWNER" == "root" && -n "${SUDO_USER:-}" ]] && DEFAULT_OWNER="$SUDO_USER"
DUMMY_KEY_OWNER="${DUMMY_KEY_OWNER:-$DEFAULT_OWNER}"

[[ -f "$STAKED_IDENTITY_KEYPAIR_PATH" ]] || { echo "error: missing staked identity keypair: $STAKED_IDENTITY_KEYPAIR_PATH" >&2; exit 1; }
case "$MODE" in
  agave)
    [[ -x "$AGAVE_BIN_PATH" ]] || { echo "error: agave-validator not executable: $AGAVE_BIN_PATH" >&2; exit 1; }
    [[ -d "$AGAVE_LEDGER_PATH" ]] || { echo "error: missing Agave ledger dir: $AGAVE_LEDGER_PATH" >&2; exit 1; }
    cmd=("$AGAVE_BIN_PATH" -l "$AGAVE_LEDGER_PATH" set-identity)
    ;;
  firedancer)
    [[ -x "$FDCTL_BIN_PATH" ]] || { echo "error: Firedancer CLI not executable: $FDCTL_BIN_PATH" >&2; exit 1; }
    [[ -f "$FD_CONFIG_PATH" ]] || { echo "error: missing Firedancer config: $FD_CONFIG_PATH" >&2; exit 1; }
    cmd=("$FDCTL_BIN_PATH" set-identity --config "$FD_CONFIG_PATH")
    ;;
esac
if [[ -x "$SOLANA_KEYGEN_PATH" ]]; then
  KEYGEN_NEW_CMD=("$SOLANA_KEYGEN_PATH" new -s --no-bip39-passphrase -o "$DUMMY_IDENTITY_KEYPAIR_PATH")
  KEYGEN_PUBKEY_CMD=("$SOLANA_KEYGEN_PATH" pubkey)
else
  [[ -x "$FDCTL_BIN_PATH" ]] || { echo "error: no keygen tool available: $SOLANA_KEYGEN_PATH or $FDCTL_BIN_PATH" >&2; exit 1; }
  KEYGEN_NEW_CMD=("$FDCTL_BIN_PATH" keys new "$DUMMY_IDENTITY_KEYPAIR_PATH")
  KEYGEN_PUBKEY_CMD=("$FDCTL_BIN_PATH" keys pubkey)
fi

if [[ -e "$IDENTITY_LINK_PATH" && ! -L "$IDENTITY_LINK_PATH" && "$ALLOW_NON_SYMLINK" -ne 1 ]]; then
  echo "error: identity path is not a symlink: $IDENTITY_LINK_PATH (set ALLOW_NON_SYMLINK=1 to override)" >&2
  exit 1
fi

if [[ ! -f "$DUMMY_IDENTITY_KEYPAIR_PATH" ]]; then
  echo "Dummy identity not found at $DUMMY_IDENTITY_KEYPAIR_PATH; generating..."
  mkdir -p "$(dirname "$DUMMY_IDENTITY_KEYPAIR_PATH")"
  "${KEYGEN_NEW_CMD[@]}"

  if [[ -n "$DUMMY_KEY_OWNER" ]]; then
    chown "$DUMMY_KEY_OWNER" "$DUMMY_IDENTITY_KEYPAIR_PATH"
  fi
  chmod 600 "$DUMMY_IDENTITY_KEYPAIR_PATH" || true
fi
STAKED_PUBKEY="$("${KEYGEN_PUBKEY_CMD[@]}" "$STAKED_IDENTITY_KEYPAIR_PATH")"

if [[ "$DUMMY_IDENTITY_KEYPAIR_PATH" == "$STAKED_IDENTITY_KEYPAIR_PATH" ]]; then
  echo "error: dummy and staked identity paths are the same: $DUMMY_IDENTITY_KEYPAIR_PATH" >&2
  exit 1
fi

DUMMY_PUBKEY="$("${KEYGEN_PUBKEY_CMD[@]}" "$DUMMY_IDENTITY_KEYPAIR_PATH")"

if [[ "$DUMMY_PUBKEY" == "$STAKED_PUBKEY" ]]; then
  echo "error: dummy and staked identities resolve to the same pubkey" >&2
  exit 1
fi

if [[ "$IDENTITY_LINK_PATH" -ef "$STAKED_IDENTITY_KEYPAIR_PATH" ]]; then
  TARGET_IDENTITY="$DUMMY_IDENTITY_KEYPAIR_PATH"
  TARGET_MODE="dummy"
  TARGET_PUBKEY="$DUMMY_PUBKEY"
elif [[ "$IDENTITY_LINK_PATH" -ef "$DUMMY_IDENTITY_KEYPAIR_PATH" ]]; then
  TARGET_IDENTITY="$STAKED_IDENTITY_KEYPAIR_PATH"
  TARGET_MODE="staked"
  TARGET_PUBKEY="$STAKED_PUBKEY"
else
  echo "error: identity symlink points to neither staked nor dummy keypair: $IDENTITY_LINK_PATH" >&2
  exit 1
fi

if [[ "$SET_IDENTITY_FORCE" -eq 1 ]]; then
  cmd+=(--force)
fi
cmd+=("$TARGET_IDENTITY")

if [[ "$SKIP_CLUSTER_CHECK" -ne 1 ]]; then
  INTERFACE_IP="$(ip -4 route get 8.8.8.8 2>/dev/null | awk '{for (i=1; i<=NF; i++) if ($i=="src") {print $(i+1); exit}}')"
  if [[ -z "$INTERFACE_IP" ]]; then
    echo "error: could not detect local IPv4 source address"
    exit 1
  fi

  RPC_CURL=(curl -s --fail --max-time 10 -X POST -H "Content-Type: application/json")
  while :; do
    CLUSTER_STATE="$("${RPC_CURL[@]}" -d '{"jsonrpc":"2.0","id":1,"method":"getClusterNodes"}' "$CLUSTER_RPC_URL" | jq --raw-output --exit-status --arg target "$TARGET_PUBKEY" --arg staked "$STAKED_PUBKEY" --arg dummy "$DUMMY_PUBKEY" --arg iface "$INTERFACE_IP" '
      .result as $nodes
      | [$nodes[] | select(.pubkey == $staked) | .gossip // empty] as $staked_gossip
      | [
          (if any($nodes[]; .pubkey == $target) then
            if any($nodes[]; .pubkey == $target and ((.gossip // "") | startswith($iface + ":"))) then
              "our"
            else
              "other"
            end
          else
            "none"
          end),
          (if any($nodes[]; .pubkey == $dummy and (.gossip as $gossip | ($staked_gossip | index($gossip)))) then
            "yes"
          else
            "no"
          end),
          ($staked_gossip | join(","))
        ]
      | @tsv
    ')" || {
      echo "error: getClusterNodes RPC failed or response could not be parsed: $CLUSTER_RPC_URL"
      exit 1
    }
    IFS=$'\t' read -r TARGET_MATCH_SCOPE DUMMY_AT_STAKED_GOSSIP STAKED_GOSSIP_ADDRS <<< "$CLUSTER_STATE"

    if [[ "$TARGET_MATCH_SCOPE" == "none" ]]; then
      if [[ "$TARGET_MODE" == "staked" ]]; then
        echo "info: staked identity $STAKED_PUBKEY is absent from getClusterNodes; trying hotswap"
      else
        break
      fi
    else
      SCOPE_DESC=$([[ "$TARGET_MATCH_SCOPE" == "our" ]] && echo "local interface $INTERFACE_IP" || echo "another interface")
      if [[ "$TARGET_MODE" != "staked" ]]; then
        echo "error: swap denied: target identity $TARGET_PUBKEY is already visible on $SCOPE_DESC; require absence from getClusterNodes"
        exit 1
      fi

      if [[ "$DUMMY_AT_STAKED_GOSSIP" == "yes" ]]; then
        echo "info: dummy identity $DUMMY_PUBKEY is visible at staked gossip address $STAKED_GOSSIP_ADDRS; trying hotswap"
      else
        CURRENT_SLOT="$("${RPC_CURL[@]}" -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}' "$CLUSTER_RPC_URL" | jq --raw-output --exit-status '.result // error(.error.message // "missing result")')" || {
          echo "error: getSlot RPC failed or response could not be parsed while checking staked promote eligibility: $CLUSTER_RPC_URL"
          exit 1
        }
        STAKED_STATE="$("${RPC_CURL[@]}" -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getVoteAccounts\",\"params\":[{\"keepUnstakedDelinquents\":true,\"delinquentSlotDistance\":$DELINQUENT_SLOT_DISTANCE}]}" "$CLUSTER_RPC_URL" | jq --raw-output --exit-status --arg pubkey "$STAKED_PUBKEY" --argjson slot "$CURRENT_SLOT" '
          (.result // error(.error.message // "missing result")) as $r
          | [
              any($r.delinquent[]?; .nodePubkey == $pubkey),
              ($slot - ($r.current + $r.delinquent | map(select(.nodePubkey == $pubkey).lastVote)[0] // 0))
            ] | @tsv
        ')" || {
          echo "error: getVoteAccounts RPC failed or response could not be parsed while checking staked promote eligibility: $CLUSTER_RPC_URL"
          exit 1
        }
        IFS=$'\t' read -r STAKED_DELINQUENT SLOT_LAG <<< "$STAKED_STATE"
        if [[ "$STAKED_DELINQUENT" == "true" ]]; then
          echo "info: staked identity $STAKED_PUBKEY is delinquent with slot lag $SLOT_LAG; trying hotswap"
        else
          echo "info: staked identity $STAKED_PUBKEY is visible on $SCOPE_DESC and is not delinquent at slot distance $DELINQUENT_SLOT_DISTANCE; slot lag $SLOT_LAG; waiting"
          sleep "$CLUSTER_POLL_INTERVAL_SECS"
          continue
        fi
      fi
    fi

    if run "${cmd[@]}"; then
      run ln -sf "$TARGET_IDENTITY" "$IDENTITY_LINK_PATH"
      exit 0
    fi
    echo "warning: set-identity failed; polling cluster and retrying" >&2
    sleep "$CLUSTER_POLL_INTERVAL_SECS"
  done
fi

if [[ "$WAIT" -eq 1 ]]; then
  if [[ "$MODE" == "agave" && "$TARGET_MODE" == "dummy" ]]; then
    run "$AGAVE_BIN_PATH" -l "$AGAVE_LEDGER_PATH" wait-for-restart-window \
      --min-idle-time "$MIN_IDLE_TIME" --skip-new-snapshot-check
  elif [[ "$MODE" == "firedancer" ]]; then
    echo "warning: --wait is only supported for agave mode; ignoring" >&2
  fi
fi
run "${cmd[@]}"
run ln -sf "$TARGET_IDENTITY" "$IDENTITY_LINK_PATH"
