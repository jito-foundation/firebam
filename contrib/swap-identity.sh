#!/usr/bin/env bash
set -euo pipefail

# swap-identity.sh
#
# Swap the identity for a single validator type (Agave or Firedancer).
# - Toggles between staked and dummy identities based on IDENTITY_LINK_PATH.
# - Promotes with --require-tower when switching to the staked identity.
# - Updates /etc/solana/identity.json symlink for restart consistency.
# - Does not copy tower files; ensure the tower exists before promotion.
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
  identity (using --require-tower).

  --wait  Wait for a restart window of $MIN_IDLE_TIME minutes before demotion
          (Agave mode only)
  --dry-run still generates the dummy identity if missing

Environment overrides (defaults shown):
  AGAVE_DIR=${HOME}/jito-solana/docker-output
  AGAVE_BIN_PATH=${AGAVE_DIR}/agave-validator
  SOLANA_KEYGEN_PATH=${AGAVE_DIR}/solana-keygen
  FD_BIN_PATH=${SCRIPT_DIR}/../build/native/gcc/bin
  FDCTL_BIN_PATH=${FD_BIN_PATH}/fddev
  FD_CONFIG_PATH=${SCRIPT_DIR}/../mainnet.toml
  AGAVE_LEDGER_PATH=/solana/ledger
  IDENTITY_LINK_PATH=/etc/solana/identity.json
  STAKED_IDENTITY_KEYPAIR_PATH=/etc/solana/staked-identity.json
  DUMMY_IDENTITY_KEYPAIR_PATH=/etc/solana/unstaked-identity.json
  MIN_IDLE_TIME=2       # minimum idle minutes for wait-for-restart-window
  SET_IDENTITY_FORCE=0  # set to 1 to add --force to set-identity
  ALLOW_NON_SYMLINK=0   # set to 1 to overwrite a non-symlink identity.json
  DUMMY_KEY_OWNER=      # optional owner for newly generated dummy key
  CLUSTER_RPC_URL=https://api.mainnet.solana.com
  SKIP_CLUSTER_CHECK=0  # set to 1 to skip checking target identity pubkey is already in use
USAGE
}

# Echo the command and execute it unless --dry-run is set.
run() {
  echo "+ $*"
  if [[ "$DRY_RUN" -eq 0 ]]; then
    "$@"
  fi
}

# Create the dummy identity keypair if missing, using solana-keygen or fdctl.
ensure_dummy_identity() {
  if [[ -f "$DUMMY_IDENTITY_KEYPAIR_PATH" ]]; then
    return 0
  fi

  echo "Dummy identity not found at $DUMMY_IDENTITY_KEYPAIR_PATH; generating..."
  local -a keygen_cmd=()
  if [[ -x "$SOLANA_KEYGEN_PATH" ]]; then
    keygen_cmd=("$SOLANA_KEYGEN_PATH" new -s --no-bip39-passphrase -o "$DUMMY_IDENTITY_KEYPAIR_PATH")
  else
    keygen_cmd=("$FDCTL_BIN_PATH" keys new "$DUMMY_IDENTITY_KEYPAIR_PATH")
  fi

  mkdir -p "$(dirname "$DUMMY_IDENTITY_KEYPAIR_PATH")"
  "${keygen_cmd[@]}"

  if [[ -n "$DUMMY_KEY_OWNER" ]]; then
    chown "$DUMMY_KEY_OWNER" "$DUMMY_IDENTITY_KEYPAIR_PATH"
  fi
  chmod 600 "$DUMMY_IDENTITY_KEYPAIR_PATH" || true
}

# https://docs.anza.xyz/operations/guides/validator-failover
swap_agave() {
  local -a cmd=()
  if [[ "$TARGET_MODE" == "dummy" && "$WAIT" -eq 1 ]]; then
    run "$AGAVE_BIN_PATH" -l "$AGAVE_LEDGER_PATH" wait-for-restart-window \
      --min-idle-time "$MIN_IDLE_TIME" --skip-new-snapshot-check
  fi

  cmd=("$AGAVE_BIN_PATH" -l "$AGAVE_LEDGER_PATH" set-identity)
  if [[ "$SET_IDENTITY_FORCE" -eq 1 ]]; then
    cmd+=(--force)
  fi
  if [[ "$TARGET_MODE" == "staked" ]]; then
    cmd+=(--require-tower)
  fi
  cmd+=("$TARGET_IDENTITY")
  run "${cmd[@]}"
  run ln -sf "$TARGET_IDENTITY" "$IDENTITY_LINK_PATH"
}

# https://docs.firedancer.io/api/cli.html#set-identity
swap_firedancer() {
  local -a cmd=()
  if [[ "$WAIT" -eq 1 ]]; then
    echo "warning: --wait is only supported for agave mode; ignoring" >&2
  fi

  cmd=("$FDCTL_BIN_PATH" set-identity --config "$FD_CONFIG_PATH")
  if [[ "$SET_IDENTITY_FORCE" -eq 1 ]]; then
    cmd+=(--force)
  fi
  cmd+=("$TARGET_IDENTITY")
  run "${cmd[@]}"
  run ln -sf "$TARGET_IDENTITY" "$IDENTITY_LINK_PATH"
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
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done
[[ -n "$MODE" ]] || { usage; exit 2; }

AGAVE_DIR="${AGAVE_DIR:-${HOME}/jito-solana/docker-output}"
AGAVE_BIN_PATH="${AGAVE_BIN_PATH:-${AGAVE_DIR}/agave-validator}"
SOLANA_KEYGEN_PATH="${SOLANA_KEYGEN_PATH:-${AGAVE_DIR}/solana-keygen}"
SCRIPT_DIR="${SCRIPT_DIR:-$(cd "$(dirname "$0")" && pwd)}"
FD_BIN_PATH="${FD_BIN_PATH:-${SCRIPT_DIR}/../build/native/gcc/bin}"
FDCTL_BIN_PATH=$([[ -x "$FD_BIN_PATH/fdctl" ]] && echo "$FD_BIN_PATH/fdctl" || echo "$FD_BIN_PATH/fddev")
FD_CONFIG_PATH="${FD_CONFIG_PATH:-${SCRIPT_DIR}/../mainnet.toml}"
AGAVE_LEDGER_PATH="${AGAVE_LEDGER_PATH:-/solana/ledger}"
IDENTITY_LINK_PATH="${IDENTITY_LINK_PATH:-/etc/solana/identity.json}"
STAKED_IDENTITY_KEYPAIR_PATH="${STAKED_IDENTITY_KEYPAIR_PATH:-/etc/solana/staked-identity.json}"
DUMMY_IDENTITY_KEYPAIR_PATH="${DUMMY_IDENTITY_KEYPAIR_PATH:-/etc/solana/unstaked-identity.json}"
MIN_IDLE_TIME="${MIN_IDLE_TIME:-2}"
SET_IDENTITY_FORCE="${SET_IDENTITY_FORCE:-0}"
ALLOW_NON_SYMLINK="${ALLOW_NON_SYMLINK:-0}"
CLUSTER_RPC_URL="${CLUSTER_RPC_URL:-https://api.mainnet.solana.com}"
SKIP_CLUSTER_CHECK="${SKIP_CLUSTER_CHECK:-0}"

DEFAULT_OWNER="$(id -un)"
[[ "$DEFAULT_OWNER" == "root" && -n "${SUDO_USER:-}" ]] && DEFAULT_OWNER="$SUDO_USER"
DUMMY_KEY_OWNER="${DUMMY_KEY_OWNER:-$DEFAULT_OWNER}"

if [[ "$MODE" == "agave" ]]; then
  [[ -x "$AGAVE_BIN_PATH" ]] || { echo "error: agave-validator not executable: $AGAVE_BIN_PATH" >&2; exit 1; }
elif [[ "$MODE" == "firedancer" ]]; then
  [[ -x "$FDCTL_BIN_PATH" ]] || { echo "error: fdctl not executable: $FDCTL_BIN_PATH" >&2; exit 1; }
  [[ -f "$FD_CONFIG_PATH" ]] || { echo "error: missing file $FD_CONFIG_PATH" >&2; exit 1; }
fi

[[ -f "$STAKED_IDENTITY_KEYPAIR_PATH" ]] || { echo "error: missing file $STAKED_IDENTITY_KEYPAIR_PATH" >&2; exit 1; }
if [[ "$MODE" == "agave" ]]; then
  [[ -d "$AGAVE_LEDGER_PATH" ]] || { echo "error: missing dir $AGAVE_LEDGER_PATH" >&2; exit 1; }
fi
if [[ ! -x "$SOLANA_KEYGEN_PATH" ]]; then
  [[ -x "$FDCTL_BIN_PATH" ]] || { echo "error: no keygen tool found (solana-keygen or fdctl)" >&2; exit 1; }
fi

if [[ -e "$IDENTITY_LINK_PATH" && ! -L "$IDENTITY_LINK_PATH" && "$ALLOW_NON_SYMLINK" -ne 1 ]]; then
  echo "error: $IDENTITY_LINK_PATH is not a symlink; set ALLOW_NON_SYMLINK=1 to override" >&2
  exit 1
fi

ensure_dummy_identity
if [[ -x "$SOLANA_KEYGEN_PATH" ]]; then
  STAKED_PUBKEY="$("$SOLANA_KEYGEN_PATH" pubkey "$STAKED_IDENTITY_KEYPAIR_PATH")"
else
  STAKED_PUBKEY="$("$FDCTL_BIN_PATH" keys pubkey "$STAKED_IDENTITY_KEYPAIR_PATH")"
fi

if [[ "$DUMMY_IDENTITY_KEYPAIR_PATH" == "$STAKED_IDENTITY_KEYPAIR_PATH" ]]; then
  echo "error: dummy and staked keypair paths are identical" >&2
  exit 1
fi

if [[ -x "$SOLANA_KEYGEN_PATH" ]]; then
  DUMMY_PUBKEY="$("$SOLANA_KEYGEN_PATH" pubkey "$DUMMY_IDENTITY_KEYPAIR_PATH")"
else
  DUMMY_PUBKEY="$("$FDCTL_BIN_PATH" keys pubkey "$DUMMY_IDENTITY_KEYPAIR_PATH")"
fi

if [[ "$DUMMY_PUBKEY" == "$STAKED_PUBKEY" ]]; then
  echo "error: dummy identity matches staked identity; check paths" >&2
  exit 1
fi

if [[ "$IDENTITY_LINK_PATH" -ef "$STAKED_IDENTITY_KEYPAIR_PATH" ]]; then
  TARGET_IDENTITY="$DUMMY_IDENTITY_KEYPAIR_PATH"
  TARGET_MODE="dummy"
elif [[ "$IDENTITY_LINK_PATH" -ef "$DUMMY_IDENTITY_KEYPAIR_PATH" ]]; then
  TARGET_IDENTITY="$STAKED_IDENTITY_KEYPAIR_PATH"
  TARGET_MODE="staked"
else
  echo "error: $IDENTITY_LINK_PATH does not point to staked or dummy identity" >&2
  exit 1
fi

if [[ "$SKIP_CLUSTER_CHECK" -ne 1 ]]; then
  CLUSTER_JSON="$(curl -s --fail --max-time 10 \
    -X POST -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","id":1,"method":"getClusterNodes"}' \
    "$CLUSTER_RPC_URL")" || {
      echo "error: failed to query cluster nodes from $CLUSTER_RPC_URL"
      exit 1
    }

  TARGET_PUBKEY=$([[ "$TARGET_MODE" == "staked" ]] && echo "$STAKED_PUBKEY" || echo "$DUMMY_PUBKEY")
  if echo "$CLUSTER_JSON" | grep -q "$TARGET_PUBKEY"; then
    echo "error: target identity $TARGET_PUBKEY already present in cluster ($CLUSTER_RPC_URL)"
    exit 1
  fi
fi

case "$MODE" in
  agave) swap_agave ;;
  firedancer) swap_firedancer ;;
esac
