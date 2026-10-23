# Audit an upstream-main candidate

Pin `old_base`, `old_tip`, `new_base`, and `new_tip`. Run
`scripts/audit_rebase.sh`. Account for every range-diff deviation, old-only or
new-only path, added file whose blob changed, and gitlink or `.gitmodules`
change.

Cleanly applied hunks matter as much as conflicts. Start from the paths both
FireBAM and the upstream range changed, read those upstream commits, then trace
each affected feature end to end:

```text
config/mode predicate -> topology links -> tile config -> producer
-> validation boundary -> consumer -> result/feedback
```

Boundaries that have broken before:

- bundle ingestion versus the shared BAM/bundle crank;
- every consumer of `tiles.bundle.enabled`, `tiles.bam.enabled` (C:
  `config->bam.tiles.enabled`), and derived predicates such as
  `TIP_CRANK_ENABLED( config )`;
- keyguard roles, payload classification, configured authorities, link MTUs;
- raw versus polled input indices, output indices, multi-worker loops, new
  topology links;
- numeric input kinds, tile metric IDs, signatures, client IDs;
- metric names: `make -C src/disco/metrics metrics` lints metrics.xml merged
  with metrics_bam.xml (generate/bam.py), so a new upstream lint rule can
  force BAM renames, which contrib/grafana must follow;
- clocks, reset/leader latching;
- BAM leader snapshots, durable result queues, ownership generations, and
  suppression of non-BAM work;
- URL/SNI limits, config validation and redaction, generated files;
- state upstream removed or refactored that FireBAM read.

When topology or shared machinery changed, check each mode:

| Bundle | BAM | Required observation |
| --- | --- | --- |
| off | off | shared machinery disabled |
| on | off | bundle behavior unchanged |
| off | on | BAM-only shared machinery fully configured |
| on | on | no duplicate or conflicting ownership |

Prefer checks that cross the changed boundary: authorize a generated crank,
publish through affected worker output indices, resolve a provisional BAM
result at Motor. Compare wire or behavior changes with `bam_spec.md`
(firebam-bam-spec-audit skill).

The audit ends with a verdict on the exact `new_tip`, listing findings, the
checks run with their results, and what was not tested. A known semantic
regression fails it even when tests pass.
