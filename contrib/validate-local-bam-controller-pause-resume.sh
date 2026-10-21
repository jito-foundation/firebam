#!/usr/bin/env bash

# Validates BAM-controller pause/resume churn against a live local validator:
# 1. initialize the local dev config;
# 2. start the benign BAM controller scenario;
# 3. stop and continue the BAM controller after the scheduler stream is active;
# 4. confirm controller-side BAM feedback resumes afterward.

set -euo pipefail
IFS=$'\n\t'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)
# shellcheck source=contrib/bam-local-lib.sh
source "${ROOT}/contrib/bam-local-lib.sh"

LIVE_EXPECT=(
  --expect-operator-pattern 'operator: signal bam stop'
  --expect-operator-pattern 'operator: signal bam cont'
)
run_live_operator_validation bam-pause-resume.txt 60 "$@"
