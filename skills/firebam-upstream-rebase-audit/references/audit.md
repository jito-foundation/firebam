# Audit an upstream-main candidate

Inspect existing pinned revisions. Read-only audits use source/history and
available validation evidence; a missing check is a reported gap, not an
instruction to rebase or publish.

## Pin immutable revisions

Record full hashes for:

- old Firedancer merge base;
- old FireBAM tip;
- new Firedancer target;
- rebased FireBAM tip;
- old Agave base and FireBAM tip;
- new Agave base and rebased tip.

Derive Agave revisions with `git ls-tree` from the pinned parent commits, not
from the current submodule checkout. Inspect affected Agave deltas and the
C/Rust boundary separately. For a publication-readiness audit, verify that the
exact gitlink commit is fetchable through the remote URL in `.gitmodules`.
A submodule branch is optional; check it only when `.gitmodules` intentionally
configures one. Report missing reachability evidence rather than publishing
a dependency as part of an audit.

## Run the deterministic audit

Set the hash variables above, then run the bundled script from the repository
root for the parent and for affected Agave history:

```bash
skills/firebam-upstream-rebase-audit/scripts/audit_rebase.sh \
  "$old_base" "$old_tip" "$new_base" "$new_tip"

skills/firebam-upstream-rebase-audit/scripts/audit_rebase.sh \
  "$old_agave_base" "$old_agave_tip" \
  "$new_agave_base" "$new_agave_tip" agave
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

For an audit, inspect source, test coverage, and available evidence for the
observations below. During migration or requested fresh validation, exercise
the affected bundle/BAM combinations when topology or shared machinery changes:

| Bundle | BAM | Required observation |
| --- | --- | --- |
| off | off | shared machinery disabled |
| on | off | bundle behavior unchanged |
| off | on | BAM-only shared machinery fully configured |
| on | on | no duplicate or conflicting ownership |

When Frankendancer plugin wiring is affected, check BAM with GUI/plugin enabled,
including `bam_plugi` producer, consumer, reliability, and polling configuration.

Check behavior beyond topology shape. Useful validation crosses the changed
boundary: authorize a generated crank, publish through affected worker output
indices, resolve a provisional BAM result at PoH, or round-trip an FFI result
variant. Assess existing coverage or exercise these paths according to scope.

For a read-only audit, inspect existing build/test evidence and report missing
coverage. Additional validation follows the requested audit scope. When
running checks, build affected tests first and use
[FireBAM testing](../../../doc/firebam-coordination.md#testing) for prerequisites.
Select BAM, topology, pack, crank/keyguard, execution, PoH/replay, config, URL,
and Agave checks according to the changed boundaries. Use supported normal-page
fallbacks when privileged prerequisites are unavailable, and report the exact
untested boundary.

## Report

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
known, even if existing tests pass. An audit completes with its findings,
evidence, and coverage gaps; an unresolved defect prevents a passing verdict.
Publication requires defects to be fixed and changed boundaries to have
suitable coverage; see [publication](publication.md).
