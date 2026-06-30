#!/usr/bin/env bash
set -euo pipefail

SOURCE=${SOURCE:-$(git rev-parse --show-toplevel)}
TARGET=${TARGET:-/home/eric/dev/firebam-early-release}
SOURCE_REF=${SOURCE_REF:-HEAD}
SOURCE_BRANCH=${SOURCE_BRANCH:-$(git -C "$SOURCE" branch --show-current)}
BRANCH=${BRANCH:-${SOURCE_BRANCH##*/}}
BASE_REF=${BASE_REF:-${BRANCH%-bam}}
PATCH=${PATCH:-/tmp/firebam-${BRANCH//\//-}-filtered.patch}
UPSTREAM=${UPSTREAM:-https://github.com/firedancer-io/firedancer.git}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<EOF
Usage: $0

Squashes SOURCE_REF from SOURCE onto BRANCH in TARGET, replacing local and remote BRANCH.

Environment defaults:
  SOURCE=$SOURCE
  TARGET=$TARGET
  SOURCE_REF=$SOURCE_REF
  BRANCH=$BRANCH
  BASE_REF=$BASE_REF
  PATCH=$PATCH
  UPSTREAM=$UPSTREAM

BASE_REF defaults to BRANCH without -bam. If missing, the newest merged non-BAM v* ref is used.
EOF
  exit 0
fi

[[ "$#" -eq 0 ]] || { echo "Usage: $0 [--help]" >&2; exit 2; }

git -C "$SOURCE" fetch --no-tags --no-recurse-submodules "$UPSTREAM" 'refs/heads/v*:refs/remotes/upstream/v*'

if git -C "$SOURCE" rev-parse --verify --quiet "upstream/$BASE_REF^{commit}" >/dev/null; then
  BASE_REF="upstream/$BASE_REF"
elif ! git -C "$SOURCE" rev-parse --verify --quiet "$BASE_REF^{commit}" >/dev/null; then
  BASE_REF=$(
    git -C "$SOURCE" for-each-ref --merged "$SOURCE_REF" --sort=-committerdate \
      --format='%(refname:short)' 'refs/heads/v*' 'refs/remotes/upstream/v*' |
    awk '!/-bam$/ { print; exit }'
  )
fi

if [[ -z "$BASE_REF" ]]; then
  echo "Unable to find a base ref for $SOURCE_REF; set BASE_REF" >&2
  exit 1
fi

MERGE_BASE=$(git -C "$SOURCE" merge-base "$BASE_REF" "$SOURCE_REF")

git -C "$SOURCE" diff --binary --full-index "$MERGE_BASE".."$SOURCE_REF" -- \
  . \
  ':(exclude)AGENTS.md' \
  ':(exclude)bam_spec.md' \
  ':(exclude)skills/**' \
  > "$PATCH"

git -C "$TARGET" fetch --no-tags --no-recurse-submodules "$UPSTREAM" "refs/heads/${BASE_REF##*/}"
git -C "$TARGET" fetch --no-tags --no-recurse-submodules origin "refs/heads/$BRANCH:refs/remotes/origin/$BRANCH" || true

git -C "$TARGET" switch -C "$BRANCH" "$MERGE_BASE"
git -C "$TARGET" apply --index --binary "$PATCH"
git -C "$TARGET" commit -m "FireBAM commit"

git -C "$TARGET" push --force-with-lease --porcelain origin "HEAD:refs/heads/$BRANCH"
