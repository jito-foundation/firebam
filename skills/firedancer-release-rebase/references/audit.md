# Audit a versioned FireBAM release candidate

This workflow inspects existing revisions, source/history, and validation
evidence. It does not invoke release preparation or publication. Inspect lane
and finalization rules in [preparation](preparation.md#respect-the-monthly-release-lanes)
when the audit concerns promotion or release-finalization differences.

## Audit every change

Pin `old_base` (previous official release in the lane), `old_tip` (prior
FireBAM tip), `new_base` (requested official release), and `new_tip` (existing
candidate). Verify the official release and lane from current metadata. Use
existing revisions for a read-only audit; derive changed gitlinks from those
parent trees and inspect affected dependency history/ABI separately.

Run the bundled helper against the candidate repository:

```bash
python3 /path/to/firedancer-release-rebase/scripts/audit_trees.py \
  --repo "$candidate_worktree" --base "$old_base" --old "$old_tip" \
  --upstream "$new_base" --new "$new_tip" > "$audit_dir/tree-audit.json"
```

Exit 0 means no non-overlap deviation; shared paths still need semantic review. Exit 2 means deviations require explanation, including intentional fixes. Exit 1 means invalid inputs/history or execution failure. The helper compares full-tree blobs, modes, types, deletions, additions, and gitlinks; it does not inspect submodule internals or prove correctness.

Read the complete `git range-diff "$old_base..$old_tip" "$new_base..$new_tip"`. Account for every modified, missing, extra, renamed, generated, or shared path. Expected non-overlap content is the new upstream tree for upstream-only changes and the previous FireBAM tree otherwise. Patch IDs support unchanged-delta claims but cannot prove semantics. Prove already-upstream behavior in the final tree.

Review interactions even without conflicts. Trace affected behavior through config, both validator topologies, links, verification/keyguard, pack, execution, PoH/replay, and BAM feedback as applicable. Recheck changed BAM limits against live node code and preserve ownership handshakes, durable results, leader timing, and mode transitions.

Record pinned hashes, commit mapping, conflicts, exceptions, submodule evidence,
commands/results, and gaps. Investigate unexplained behavior, hunks, or production
changes; unresolved findings prevent a passing verdict. Inspect validation
evidence for the exact candidate, including compiler/concurrency settings and
relevant regressions. Report missing or stale evidence and unresolved
dependencies. Choose fresh checks according to the requested audit scope and
existing authorization; missing checks are reportable gaps. An audit completes
with these findings. A passing publication-readiness verdict requires the release
gates in [preparation](preparation.md#build-the-exact-candidate); assess evidence
for those gates without invoking rebase or publication.
