---
name: firedancer-release-rebase
description: Rebase FireBAM onto a versioned upstream Firedancer release (a monthly YY.MM lane), including syncing a lane such as v26.10 to a new patch release, audit such a candidate, or publish it. Upstream main uses firebam-upstream-rebase-audit.
---

# Firedancer release rebase

A release lane is FireBAM rebased onto an official `firedancer-io/firedancer`
release tag. Lane rules:

- A lane carries only code that `main` already ships, copied verbatim. A fix
  that is not on `main` lands there first; never write lane-only code.
- A lane is the official tag followed only by FireBAM commits, with no merge
  commits: `git rev-list --merges "$new_base..$new_tip"` prints nothing. A sync
  replaces the lane branch with a leased force-push. Never merge the release
  into a lane, even to let a pull request land without a force-push.
- Testnet and mainnet are separate lanes within a month; tell them apart from
  release metadata and history, not the patch suffix. A mainnet promotion adds
  no application change over the qualified testnet tag. Compare both the
  official and the FireBAM candidates with the testnet tags; only version
  metadata and binary finalization may differ.
- Use the exact requested release, never upstream `main` or the latest
  release. Find the lane's previous release (`old_base`) from release history
  and ancestry, not `merge-base` alone.
- Activate a feature only when both clients support it; the Agave release
  calendar is not a signal.
- A fresh `make -j"$(nproc)" all` must pass the build gate, including
  unit-test and fuzz targets. Default validator builds alone do not cover them.

References:

- [Preparation](references/preparation.md): fetching the official tag, the
  rebase, finalization, and the build gate.
- [Audit](references/audit.md): `scripts/audit_trees.py` and what it proves.
  The boundary list and mode matrix in
  `skills/firebam-upstream-rebase-audit/references/audit.md` apply here too.
- [Publication](references/publication.md): branch, tag, and GitHub release.

Before publishing, get an independent read-only review of the exact candidate
and iterate until it has no actionable findings. Publishing a release means the
branch push, the new tag, and the GitHub release. It is done when the branch and
the peeled tag equal `new_tip`, the release matches the lane's template, and
changed gitlinks are fetchable.
