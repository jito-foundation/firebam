#!/usr/bin/env bash
set -euo pipefail

# swap-identity.sh
#
# Swap the staked identity between an Agave validator and Firedancer.
# - Demotes the current active validator to a dummy identity.
# - Copies the tower file to the target ledger.
# - Promotes the target validator with --require-tower.
# - Updates /etc/solana/identity.json symlink for restart consistency.
#
# This script does not stop or start systemd services; it only performs
# set-identity and file operations. Use --dry-run to inspect commands.
#
# WARNING: If both validators use the same IDENTITY_LINK_PATH and the
# inactive service restarts while that link points at the staked keypair,
# both validators can sign with the same identity (slashing risk).
# Prefer separate identity symlinks per service.

# ---- helpers ----
usage() {
  cat <<'USAGE'
Usage: swap-identity.sh to-firedancer|to-agave [--dry-run] [--wait]

  --wait  Wait for a restart window of $MIN_IDLE_TIME minutes before demotion

Environment overrides (defaults shown):
  AGAVE_BIN_PATH=${HOME}/jito-solana/docker-output/agave-validator #TODO: fix these paths
  FDCTL_BIN_PATH=${HOME}/firebam/build/native/gcc/bin/fdctl
  FD_CONFIG_PATH=${HOME}/firebam/mainnet.toml
  AGAVE_LEDGER_PATH=/solana/ledger
  FD_LEDGER_PATH=/solana/ledger/firebam
  IDENTITY_LINK_PATH=/etc/solana/identity.json
  STAKED_IDENTITY_KEYPAIR_PATH=/etc/solana/staked-identity.json
  DUMMY_IDENTITY_KEYPAIR_PATH=/etc/solana/unstaked-identity.json
  MIN_IDLE_TIME=2       # minimum idle minutes for wait-for-restart-window
  SET_IDENTITY_FORCE=0  # set to 1 to add --force to set-identity
  ALLOW_NON_SYMLINK=0   # set to 1 to overwrite a non-symlink identity.json
  DUMMY_KEY_OWNER=      # optional owner for newly generated dummy key
USAGE
}

# Echo the command and execute it unless --dry-run is set.
run() {
  echo "+ $*"
  if [[ "$DRY_RUN" -eq 0 ]]; then
    "$@"
  fi
}

# Create the dummy identity keypair if missing, using agave-keygen or fdctl.
ensure_dummy_identity() {
  if [[ -f "$DUMMY_IDENTITY_KEYPAIR_PATH" ]]; then
    return 0
  fi

  echo "Dummy identity not found at $DUMMY_IDENTITY_KEYPAIR_PATH; generating..."
  local -a keygen_cmd=()
  if command -v agave-keygen >/dev/null 2>&1; then
    keygen_cmd=(agave-keygen new -s --no-bip39-passphrase -o "$DUMMY_IDENTITY_KEYPAIR_PATH")
  else
    keygen_cmd=("$FDCTL_BIN_PATH" keys new "$DUMMY_IDENTITY_KEYPAIR_PATH")
  fi

  run mkdir -p "$(dirname "$DUMMY_IDENTITY_KEYPAIR_PATH")"
  run "${keygen_cmd[@]}"

  if [[ -n "$DUMMY_KEY_OWNER" ]]; then
    run chown "$DUMMY_KEY_OWNER" "$DUMMY_IDENTITY_KEYPAIR_PATH"
  fi
  run chmod 600 "$DUMMY_IDENTITY_KEYPAIR_PATH" || true
}

swap_to_firedancer() {
  if [[ "$WAIT" -eq 1 ]]; then
    run "$AGAVE_BIN_PATH" -l "$AGAVE_LEDGER_PATH" wait-for-restart-window \
      --min-idle-time "$MIN_IDLE_TIME" --skip-new-snapshot-check
  fi
  # Demote Agave to dummy identity before transferring the tower.
  if [[ "$SET_IDENTITY_FORCE" -eq 1 ]]; then
    run "$AGAVE_BIN_PATH" -l "$AGAVE_LEDGER_PATH" set-identity --force "$DUMMY_IDENTITY_KEYPAIR_PATH"
  else
    run "$AGAVE_BIN_PATH" -l "$AGAVE_LEDGER_PATH" set-identity "$DUMMY_IDENTITY_KEYPAIR_PATH"
  fi
  run ln -sf "$DUMMY_IDENTITY_KEYPAIR_PATH" "$IDENTITY_LINK_PATH"

  local tower_src
  # Select the most recent tower file for the staked identity.
  tower_src="$(ls -t "$AGAVE_LEDGER_PATH"/tower-*-"$STAKED_PUBKEY".bin 2>/dev/null | head -n1)"
  [[ -n "$tower_src" ]] || { echo "error: tower for $STAKED_PUBKEY not found in $AGAVE_LEDGER_PATH" >&2; exit 1; }
  run cp -f "$tower_src" "$FD_LEDGER_PATH/"

  # Promote Firedancer to the staked identity only after tower sync.
  if [[ "$SET_IDENTITY_FORCE" -eq 1 ]]; then
    run "$FDCTL_BIN_PATH" set-identity --config "$FD_CONFIG_PATH" --force --require-tower "$STAKED_IDENTITY_KEYPAIR_PATH"
  else
    run "$FDCTL_BIN_PATH" set-identity --config "$FD_CONFIG_PATH" --require-tower "$STAKED_IDENTITY_KEYPAIR_PATH"
  fi
  run ln -sf "$STAKED_IDENTITY_KEYPAIR_PATH" "$IDENTITY_LINK_PATH"
}

swap_to_agave() {
  if [[ "$WAIT" -eq 1 ]]; then
    run "$AGAVE_BIN_PATH" -l "$FD_LEDGER_PATH" wait-for-restart-window \
      --min-idle-time "$MIN_IDLE_TIME" --skip-new-snapshot-check
  fi
  # Demote Firedancer to dummy identity before transferring the tower.
  if [[ "$SET_IDENTITY_FORCE" -eq 1 ]]; then
    run "$FDCTL_BIN_PATH" set-identity --config "$FD_CONFIG_PATH" --force "$DUMMY_IDENTITY_KEYPAIR_PATH"
  else
    run "$FDCTL_BIN_PATH" set-identity --config "$FD_CONFIG_PATH" "$DUMMY_IDENTITY_KEYPAIR_PATH"
  fi
  run ln -sf "$DUMMY_IDENTITY_KEYPAIR_PATH" "$IDENTITY_LINK_PATH"

  local tower_src
  # Select the most recent tower file for the staked identity.
  tower_src="$(ls -t "$FD_LEDGER_PATH"/tower-*-"$STAKED_PUBKEY".bin 2>/dev/null | head -n1)"
  [[ -n "$tower_src" ]] || { echo "error: tower for $STAKED_PUBKEY not found in $FD_LEDGER_PATH" >&2; exit 1; }
  run cp -f "$tower_src" "$AGAVE_LEDGER_PATH/"

  # Promote Agave to the staked identity only after tower sync.
  if [[ "$SET_IDENTITY_FORCE" -eq 1 ]]; then
    run "$AGAVE_BIN_PATH" -l "$AGAVE_LEDGER_PATH" set-identity --force --require-tower "$STAKED_IDENTITY_KEYPAIR_PATH"
  else
    run "$AGAVE_BIN_PATH" -l "$AGAVE_LEDGER_PATH" set-identity --require-tower "$STAKED_IDENTITY_KEYPAIR_PATH"
  fi
  run ln -sf "$STAKED_IDENTITY_KEYPAIR_PATH" "$IDENTITY_LINK_PATH"
}

# ---- main ----
MODE=""
DRY_RUN=0
WAIT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    to-firedancer|to-agave) MODE="$1"; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --wait) WAIT=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done
[[ -n "$MODE" ]] || { usage; exit 2; }

AGAVE_BIN_PATH="${AGAVE_BIN_PATH:-${HOME}/jito-solana/docker-output/agave-validator}" # TODO: fix these paths
FDCTL_BIN_PATH="${FDCTL_BIN_PATH:-${HOME}/firebam/build/native/gcc/bin/fdctl}"
FD_CONFIG_PATH="${FD_CONFIG_PATH:-${HOME}/firebam/mainnet.toml}"
AGAVE_LEDGER_PATH="${AGAVE_LEDGER_PATH:-/solana/ledger}"
FD_LEDGER_PATH="${FD_LEDGER_PATH:-/solana/ledger/firebam}"
IDENTITY_LINK_PATH="${IDENTITY_LINK_PATH:-/etc/solana/identity.json}"
STAKED_IDENTITY_KEYPAIR_PATH="${STAKED_IDENTITY_KEYPAIR_PATH:-/etc/solana/staked-identity.json}"
DUMMY_IDENTITY_KEYPAIR_PATH="${DUMMY_IDENTITY_KEYPAIR_PATH:-/etc/solana/unstaked-identity.json}"
MIN_IDLE_TIME="${MIN_IDLE_TIME:-2}"
SET_IDENTITY_FORCE="${SET_IDENTITY_FORCE:-0}"
ALLOW_NON_SYMLINK="${ALLOW_NON_SYMLINK:-0}"

DEFAULT_OWNER="$(id -un)"
[[ "$DEFAULT_OWNER" == "root" && -n "${SUDO_USER:-}" ]] && DEFAULT_OWNER="$SUDO_USER"
DUMMY_KEY_OWNER="${DUMMY_KEY_OWNER:-$DEFAULT_OWNER}"

if [[ ! -x "$FDCTL_BIN_PATH" ]] && command -v fdctl >/dev/null 2>&1; then
  FDCTL_BIN_PATH="$(command -v fdctl)"
fi

[[ -x "$AGAVE_BIN_PATH" ]] || { echo "error: agave-validator not executable: $AGAVE_BIN_PATH" >&2; exit 1; }
[[ -x "$FDCTL_BIN_PATH" ]] || { echo "error: fdctl not executable: $FDCTL_BIN_PATH" >&2; exit 1; }

[[ -f "$STAKED_IDENTITY_KEYPAIR_PATH" ]] || { echo "error: missing file $STAKED_IDENTITY_KEYPAIR_PATH" >&2; exit 1; }
[[ -d "$AGAVE_LEDGER_PATH" ]] || { echo "error: missing dir $AGAVE_LEDGER_PATH" >&2; exit 1; }
[[ -d "$FD_LEDGER_PATH" ]] || { echo "error: missing dir $FD_LEDGER_PATH" >&2; exit 1; }

if [[ -e "$IDENTITY_LINK_PATH" && ! -L "$IDENTITY_LINK_PATH" && "$ALLOW_NON_SYMLINK" -ne 1 ]]; then
  echo "error: $IDENTITY_LINK_PATH is not a symlink; set ALLOW_NON_SYMLINK=1 to override" >&2
  exit 1
fi

ensure_dummy_identity
if command -v agave-keygen >/dev/null 2>&1; then
  STAKED_PUBKEY="$(agave-keygen pubkey "$STAKED_IDENTITY_KEYPAIR_PATH")"
else
  STAKED_PUBKEY="$("$FDCTL_BIN_PATH" keys pubkey "$STAKED_IDENTITY_KEYPAIR_PATH")"
fi

if [[ "$DUMMY_IDENTITY_KEYPAIR_PATH" == "$STAKED_IDENTITY_KEYPAIR_PATH" ]]; then
  echo "error: dummy and staked keypair paths are identical" >&2
  exit 1
fi

if command -v agave-keygen >/dev/null 2>&1; then
  DUMMY_PUBKEY="$(agave-keygen pubkey "$DUMMY_IDENTITY_KEYPAIR_PATH")"
else
  DUMMY_PUBKEY="$("$FDCTL_BIN_PATH" keys pubkey "$DUMMY_IDENTITY_KEYPAIR_PATH")"
fi

if [[ "$DUMMY_PUBKEY" == "$STAKED_PUBKEY" ]]; then
  echo "error: dummy identity matches staked identity; check paths" >&2
  exit 1
fi

case "$MODE" in
  to-firedancer) swap_to_firedancer ;;
  to-agave) swap_to_agave ;;
esac
