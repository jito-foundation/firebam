---
name: firebam-upstream-rebase-audit
description: Plan, execute, or verify FireBAM rebases onto Firedancer upstream branches, including the nested Agave gitlink. Use for upstream main updates, rebase correctness audits, conflict review, already-upstream change detection, and checking for lost files, hunks, or cross-subsystem regressions. Do not use for ordinary BAM protocol debugging that does not involve an upstream migration.
---

# FireBAM Upstream Rebase Audit

Treat a successful Git rebase as the start of verification, not proof that the
result is correct. Audit both the migrated FireBAM delta and new upstream code
that consumes, validates, authorizes, or gates BAM behavior.

## Select the workflow

- For planning or executing a `main` rebase, follow the machine procedure
  below through pinning, Agave-first migration, parent migration, validation,
  and exact-lease publication.
- For auditing an already-created candidate, pin the four parent revisions and
  four Agave revisions, run the mechanical audit below, then perform the
  semantic review and tests required by the changed boundaries.
- Do not publish, force-push, change a canonical branch, or create external
  review artifacts unless the user has authorized that action. Planning and
  read-only verification do not imply publication authority.

Use isolated worktrees when the checkout is dirty. Never clean, reset, or
stash unrelated user files to perform a rebase.

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
  rebase --empty=stop --onto "$new_agave_base" "$old_agave_base"

new_agave_tip="$(git -C "$agave_worktree" rev-parse HEAD)"
```

If `old_agave_base==old_agave_tip`, skip the Agave worktree/rebase and set
`new_agave_tip="$new_agave_base"`. If Agave BAM commits remain, push their
candidate branch and verify the exact tip is advertised before updating the
parent gitlink.

Rebase the parent candidate and update its Agave gitlink and `.gitmodules` to
the verified Agave candidate:

```bash
git -C "$parent_worktree" \
  rebase --empty=stop --onto "$new_base" "$old_base"
new_tip="$(git -C "$parent_worktree" rev-parse HEAD)"
```

For each conflict, inspect `git rebase --show-current-patch`, unresolved paths,
all three index stages, upstream history, callers, and tests. Do not select an
entire side when both sides changed behavior. Record every skipped/empty
commit and every significant manual resolution.

Build and test in the candidate worktree. At minimum, build both validators
and the focused BAM, topology, pack, crank/keyguard, execution, PoH/replay,
config, URL, GUI, resolver, and Agave targets. Run the stateful BAM corpus and
the affected unit tests; follow `AGENTS.md` for full-suite huge-page and
memlock setup.

Before publication, fetch `origin/main` again and require it to equal
`old_tip`. Publish only with the exact lease:

```bash
git fetch origin main
test "$(git rev-parse origin/main)" = "$old_tip"
git push --force-with-lease="refs/heads/main:$old_tip" \
  origin "$new_tip:main"
```

If the equality check or lease fails, stop and reconcile the new remote work.

## Pin immutable revisions

Record full hashes for:

- old Firedancer merge base;
- old FireBAM tip;
- new Firedancer target;
- rebased FireBAM tip;
- old Agave base and FireBAM tip;
- new Agave base and rebased tip.

Derive Agave revisions with `git ls-tree` from the pinned parent commits, not
from the current submodule checkout. Rebase and publish Agave first when any
BAM delta remains. Before publishing the parent, verify that the exact gitlink
commit is advertised by the remote URL recorded in `.gitmodules`. A submodule
branch is optional and should be checked only when `.gitmodules` intentionally
configures one.

## Run the deterministic audit

Run the bundled script once for the parent and once for Agave:

```bash
skills/firebam-upstream-rebase-audit/scripts/audit_rebase.sh \
  <old-base> <old-tip> <new-base> <new-tip>

skills/firebam-upstream-rebase-audit/scripts/audit_rebase.sh \
  <old-agave-base> <old-agave-tip> \
  <new-agave-base> <new-agave-tip> agave
```

Read every `range-diff` deviation. Account explicitly for:

- old-only and new-only delta paths;
- additions, deletions, renames, copies, type changes, and mode changes;
- common added files whose final blob or mode differs;
- changed patch IDs, skipped commits, and commits made empty;
- dropped hunks and their exact upstream equivalents;
- submodule URL, branch, and gitlink changes;
- generated artifacts and their authoritative source definitions.

The script supplies evidence, not a correctness verdict. A path disappearing
from the local delta is acceptable only when the final tree and upstream
history prove that equivalent behavior is already present.

## Audit upstream interactions

Start with paths changed by both the old FireBAM delta and the upstream range.
Review upstream commits on those paths, including cleanly applied hunks. Then
trace every affected feature end to end:

```text
config/mode predicate -> topology objects and links -> tile configuration
-> producer -> security/validation boundary -> consumer -> result/feedback
```

Prioritize these failure-prone boundaries:

- bundle ingestion versus shared BAM/bundle crank enablement;
- every consumer of `tiles.bundle.enabled`, `tiles.bam.enabled`, and derived
  predicates such as `tip_crank_enabled`;
- keyguard roles, payload classification, configured authorities, and link
  MTUs;
- raw versus polled input indices, output indices, multi-worker loops, and
  newly added topology links;
- numeric input kinds, tile metric IDs, signatures, and client IDs;
- clocks, reset/leader latching, and nominal versus adjusted slot duration;
- BAM leader snapshots, durable result queues, ownership generations, and
  suppression of non-BAM work;
- C/Rust FFI ordering, widths, ownership, transaction variants, and all call
  sites after an Agave update;
- URL/SNI limits, config validation/redaction, and generated files;
- state removed or refactored by upstream that FireBAM previously read.

When a deviation changes BAM wire or behavioral semantics, compare the result
against the tracked `bam_spec.md` and distinguish required behavior from an
implementation-specific choice.

## Verify supported modes and real boundaries

Exercise all bundle/BAM combinations:

| Bundle | BAM | Required observation |
| --- | --- | --- |
| off | off | shared machinery disabled |
| on | off | bundle behavior unchanged |
| off | on | BAM-only shared machinery fully configured |
| on | on | no duplicate or conflicting ownership |

For Frankendancer, also exercise BAM with GUI/plugin enabled and verify
`bam_plugi` producer, consumer, reliability, and polling configuration.

Do not stop at topology shape. Drive operations across changed boundaries,
such as authorizing an actual generated crank, publishing through every worker
output index, resolving a provisional BAM result at PoH, or round-tripping an
FFI result variant.

Build affected tests before running them and follow `AGENTS.md` for memlock and
huge-page setup. At minimum, build both validator binaries and run focused
BAM, topology, pack, crank/keyguard, execution, PoH/replay, config, URL, and
Agave checks. If privileged prerequisites are unavailable, use supported
normal-page fallbacks and report the exact untested boundary.

## Report and publication gate

The completion report must contain:

- all pinned hashes and old-to-new commit mappings;
- file/status/blob accounting and every exception;
- already-upstream commits or partial hunks;
- significant conflict resolutions and non-conflicting upstream guards;
- Agave ABI, client-ID, gitlink, and remote-reachability evidence;
- exact commands and pass/fail results;
- unresolved defects and coverage gaps;
- a correctness verdict for the exact candidate hash.

Do not declare a candidate correct when a significant semantic regression is
known, even if existing tests pass. Do not publish until the defect is fixed
and the changed boundary has suitable coverage. Canonical branch replacement
must use an exact force-with-lease against the pinned old remote tip.
