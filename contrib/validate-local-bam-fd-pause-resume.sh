#!/usr/bin/env bash

# Validates validator pause/resume churn against a live local validator:
# 1. initialize the local dev config;
# 2. start the benign BAM controller scenario;
# 3. stop and continue the validator once BAM decode has begun;
# 4. confirm validator-side BAM activity resumes afterward.

set -euo pipefail
IFS=$'\n\t'

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)
# shellcheck source=contrib/bam-local-lib.sh
source "${ROOT}/contrib/bam-local-lib.sh"

LIVE_EXPECT=(
  --expect-operator-pattern 'operator: signal fd stop'
  --expect-operator-pattern 'operator: signal fd cont'
)
run_live_operator_validation fd-pause-resume.txt 60 "$@"
