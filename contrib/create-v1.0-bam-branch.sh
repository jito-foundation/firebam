#!/usr/bin/env bash
set -euo pipefail

SOURCE=${SOURCE:-/home/eric/dev/firebam2}
TARGET=${TARGET:-/home/eric/dev/firebam-early-release}
BRANCH=${BRANCH:-v1.0-bam}
PATCH=${PATCH:-/tmp/firebam-v1.0-bam-filtered.patch}
UPSTREAM=${UPSTREAM:-https://github.com/firedancer-io/firedancer.git}

git -C "$SOURCE" diff --binary --full-index v1.0..HEAD -- \
  . \
  ':(exclude)AGENTS.md' \
  ':(exclude)bam_spec.md' \
  ':(exclude)skills/**' \
  > "$PATCH"

git -C "$TARGET" fetch --no-tags "$UPSTREAM" refs/heads/v1.0
BASE_SHA=$(git -C "$TARGET" rev-parse FETCH_HEAD)

git -C "$TARGET" switch -c "$BRANCH" "$BASE_SHA"
git -C "$TARGET" apply --index --binary "$PATCH"
git -C "$TARGET" commit -m "FireBAM commit"

git -C "$TARGET" push --porcelain origin "${BASE_SHA}:refs/heads/$BRANCH"
git -C "$TARGET" push --porcelain origin "HEAD:refs/heads/$BRANCH"
