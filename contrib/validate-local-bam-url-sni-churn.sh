#!/usr/bin/env bash

# Validates BAM runtime URL / SNI churn against a live local validator:
# 1. initialize the local dev config;
# 2. start the benign BAM controller scenario;
# 3. move the validator to an unreachable BAM URL with a new SNI;
# 4. restore the URL and then restore the original SNI;
# 5. confirm the live node reconnects after both recovery steps.

set -euo pipefail
IFS=$'\n\t'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)
# shellcheck source=contrib/bam-local-lib.sh
source "${ROOT}/contrib/bam-local-lib.sh"

LIVE_EXPECT=(
  --expect-fd-pattern 'BAM gRPC connect attempt failed .*50056'
  --expect-bam-count '^InitSchedulerStream:' 3
  --expect-operator-pattern 'BAM runtime configuration updated \(enabled=true, url=http://127\.0\.0\.1:50056, sni=churn\.invalid\)'
  --expect-operator-pattern 'BAM runtime configuration updated \(enabled=true, url=http://127\.0\.0\.1:50055, sni=churn\.invalid\)'
  --expect-operator-pattern 'BAM runtime configuration updated \(enabled=true, url=http://127\.0\.0\.1:50055, sni=127\.0\.0\.1\)'
)
run_live_operator_validation url-sni-churn.txt 60 "$@"
