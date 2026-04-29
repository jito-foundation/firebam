#!/usr/bin/env bash

set -euo pipefail

URL="http://127.0.0.1:4000/clusterInfo"
FD_NAME="fd1"
FD_ROOT="/home/$(id -un "${UID}")/.firedancer"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Fetch clusterInfo metadata and write Firedancer key files.

Options:
    -h, --help          Show this help message and exit
    --url URL           Metadata endpoint URL
                        (default: ${URL})
    --name NAME         Firedancer instance name
                        (default: ${FD_NAME})
    --fd-root PATH      Firedancer root directory
                        (default: ${FD_ROOT})
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --url)
            URL="$2"
            shift 2
            ;;
        --name)
            FD_NAME="$2"
            shift 2
            ;;
        --fd-root)
            FD_ROOT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

TARGET_DIR="${FD_ROOT}/${FD_NAME}"

python3 - "${URL}" "${TARGET_DIR}" <<'PY'
import json
import os
import sys
from pathlib import Path
from urllib.request import urlopen

url = sys.argv[1]
target_dir = Path(sys.argv[2])
payload = json.loads(urlopen(url).read())
validators = payload.get("validators") or []
if not validators:
    raise SystemExit("clusterInfo payload is missing validators[]")
skipped_validator = validators[-1]

target_dir.mkdir(parents=True, exist_ok=True)

identity_path = target_dir / "identity.json"
vote_account_path = target_dir / "vote-account.json"
entrypoint_path = target_dir / "entrypoint"
config_path = target_dir / "local-cluster.toml"

identity_path.write_text(json.dumps(skipped_validator["node_keypair"]) + "\n")
vote_account_path.write_text(json.dumps(skipped_validator["vote_keypair"]) + "\n")
entrypoint_path.write_text(payload["entrypoint"] + "\n")
config_path.write_text(
    "[gossip]\n"
    f'entrypoints = ["{payload["entrypoint"]}"]\n\n'
    "[consensus]\n"
    f'identity_path = "{identity_path}"\n'
    f'vote_account_path = "{vote_account_path}"\n'
)

os.chmod(identity_path, 0o600)
os.chmod(vote_account_path, 0o600)
PY

echo "Wrote ${TARGET_DIR}/identity.json"
echo "Wrote ${TARGET_DIR}/vote-account.json"
echo "Wrote ${TARGET_DIR}/entrypoint"
echo "Wrote ${TARGET_DIR}/local-cluster.toml"
