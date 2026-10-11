#!/usr/bin/env bash

# Validates validator restart churn against a live local validator:
# 1. initialize the local dev config;
# 2. start the benign BAM controller scenario;
# 3. restart the validator once the first BAM scheduler stream is healthy;
# 4. confirm the validator performs a fresh BAM handshake after restart.

set -euo pipefail
IFS=$'\n\t'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)
# shellcheck source=contrib/bam-local-lib.sh
source "${ROOT}/contrib/bam-local-lib.sh"

LIVE_EXPECT=(
  --expect-bam-count '^GetAuthChallenge:' 2
  --expect-bam-count '^InitSchedulerStream:' 2
  --expect-fd-count 'BAM identity pubkey updated to' 2
  --expect-bam-pattern 'scheduler stream closed by validator conn=1'
  --expect-operator-pattern 'operator: restart_fd stopping pid='
  --expect-operator-pattern 'operator: restart_fd started pid='
)
run_live_operator_validation fd-restart.txt 120 "$@"
