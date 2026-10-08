---
name: firebam-upstream-rebase-audit
description: Rebase FireBAM onto Firedancer upstream main, audit such a candidate, or publish it to main. Versioned releases use firedancer-release-rebase.
---

# FireBAM upstream-main rebase

`main` is upstream Firedancer plus one signed commit whose message is
`firebam`, with any Agave gitlink change. A rebase replays that commit onto the
new upstream; fold follow-up fixes into it. The candidate is correct when BAM
behaves as on the old tip and everything else behaves as upstream.

Rules the rebase must keep:

- Small upstream delta. Count the lines FireBAM changes in upstream-owned files
  and compare with the old tip:
  `git diff --numstat "$new_base" "$new_tip" -- $(git ls-tree -r --name-only "$new_base") | awk '{n+=$1+$2} END {print n}'`
- BAM logic lives in FireBAM-owned `*_bam.{c,h}` files reached from one-line
  hooks in upstream files. FireBAM tests live in FireBAM-owned files, never
  inside upstream test bodies.
- Upstream bugs that BAM doesn't need go upstream as PRs, not into `firebam`.

References:

- [Migration](references/migration.md): Agave first, then the parent.
- [Audit](references/audit.md): boundaries that upstream changes break, the
  bundle/BAM mode matrix, and the verdict.
- [Publication](references/publication.md): pushing `main`.
- `scripts/audit_rebase.sh old_base old_tip new_base new_tip [repo]` prints
  pinned revisions, delta path-set differences, paths both sides changed with
  the upstream commits on them, the range-diff, and submodule changes. It is
  evidence, not a verdict.
