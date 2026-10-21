#!/usr/bin/env bash

# Validates BAM-controller hard restart churn against a live local validator:
# 1. initialize the local dev config;
# 2. start the benign BAM controller scenario;
# 3. restart the BAM controller once the first scheduler stream is healthy;
# 4. confirm the validator performs a fresh BAM handshake after the controller restarts.

set -euo pipefail
IFS=$'\n\t'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)
# shellcheck source=contrib/bam-local-lib.sh
source "${ROOT}/contrib/bam-local-lib.sh"

LIVE_EXPECT=(
  --expect-bam-count '^GetAuthChallenge:' 2
  --expect-bam-count '^GetBuilderConfig:' 2
  --expect-bam-count '^InitSchedulerStream:' 2
  --expect-bam-count 'scheduler<-validator auth proof' 2
  --expect-fd-pattern 'Disconnected: recvmsg error'
  --expect-operator-pattern 'operator: restart_bam stopping pid='
  --expect-operator-pattern 'operator: restart_bam started pid='
)
run_live_operator_validation bam-restart.txt 120 "$@"
