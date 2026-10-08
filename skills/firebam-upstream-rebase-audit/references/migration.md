# Perform an upstream-main migration

Use this procedure when the user requests a migration. Commands are examples
run from the repository root or the indicated candidate worktree. Publication
is a separate, authorized phase.

## Execute a `main` rebase

Fetch and pin the parent inputs before changing history:

```bash
git fetch --prune upstream main
git fetch --prune origin main

old_tip="$(git rev-parse origin/main)"
new_base="$(git rev-parse upstream/main)"
old_base="$(git merge-base "$old_tip" "$new_base")"
rebase_stamp="YYYYMMDD-HHMMSS"
repo_root="$(git rev-parse --show-toplevel)"
worktree_root="$(dirname "$repo_root")"
parent_worktree="$worktree_root/firebam-main-rebase-$rebase_stamp"
agave_worktree="$worktree_root/agave-main-rebase-$rebase_stamp"

git show -s --format='%H %ci %s' "$old_base"
git show -s --format='%H %ci %s' "$old_tip"
git show -s --format='%H %ci %s' "$new_base"
git log --reverse --oneline "$old_base..$old_tip"
git cherry -v "$new_base" "$old_tip"

git branch "backup/main-before-upstream-$rebase_stamp" "$old_tip"
git worktree add -b "rebase/main-upstream-$rebase_stamp" \
  "$parent_worktree" "$old_tip"
```

Derive Agave inputs from the pinned parent commits, then rebase Agave first:

```bash
old_agave_base="$(git ls-tree "$old_base" agave | awk '{print $3}')"
old_agave_tip="$(git ls-tree "$old_tip" agave | awk '{print $3}')"
new_agave_base="$(git ls-tree "$new_base" agave | awk '{print $3}')"

git -C agave fetch --prune origin
git -C agave fetch --prune upstream
git -C agave cat-file -e "$old_agave_base^{commit}"
git -C agave cat-file -e "$old_agave_tip^{commit}"
git -C agave cat-file -e "$new_agave_base^{commit}"

git -C agave branch \
  "backup/main-bam-before-upstream-$rebase_stamp" "$old_agave_tip"
git -C agave worktree add \
  -b "rebase/main-bam-upstream-$rebase_stamp" \
  "$agave_worktree" "$old_agave_tip"
git -C "$agave_worktree" \
  rebase --no-update-refs --empty=stop --onto "$new_agave_base" "$old_agave_base"

new_agave_tip="$(git -C "$agave_worktree" rev-parse HEAD)"
```

If `old_agave_base==old_agave_tip`, skip the Agave worktree/rebase and set
`new_agave_tip="$new_agave_base"`. If Agave BAM commits remain, prepare their
candidate first. Publish the dependency only within the task's publication
authorization, then verify its
exact commit is fetchable through the final submodule URL before publishing
the parent. Local migration and audit can proceed before publication.

Rebase the parent candidate and update its Agave gitlink and `.gitmodules` to
the verified Agave candidate:

```bash
git -C "$parent_worktree" \
  rebase --no-update-refs --empty=stop --onto "$new_base" "$old_base"
new_tip="$(git -C "$parent_worktree" rev-parse HEAD)"
```

For each conflict, inspect `git rebase --show-current-patch`, unresolved paths,
all three index stages, upstream history, callers, and tests. Do not select an
entire side when both sides changed behavior. Record every skipped/empty
commit and every significant manual resolution.

Validate in the candidate worktree. A completed migration must build both
validator binaries. Select regression checks for changed behavior across BAM,
topology, pack, crank/keyguard, execution, PoH/replay, config, URL, GUI,
resolver, and Agave boundaries. Run the stateful BAM corpus when the pipeline
is affected. Use the [audit guidance](audit.md#verify-supported-modes-and-real-boundaries)
and [local test setup](../../../doc/firebam-coordination.md#testing) as needed.
Keep `--no-update-refs` explicit so inherited Git configuration cannot rewrite
other branch or backup refs.
