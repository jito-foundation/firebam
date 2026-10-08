---
name: firedancer-release-rebase
description: Prepare or audit FireBAM upgrades to a specified upstream Firedancer release. Use for versioned release rebases and authorized publication; upstream main uses firebam-upstream-rebase-audit.
---

# Firedancer Release Rebase

Preserve intentional FireBAM behavior while using the exact requested official
release and promotion lane. A clean rebase and successful build are evidence,
not a correctness verdict. Follow the user's scope and existing authorization.

## Select the workflow

- **Plan:** Identify the requested release, destination branch/lane, existing
  revisions, changed boundaries, and validation needs. Consult procedure
  details only as needed to explain the plan; planning does not make a release.
- **Audit an existing candidate:** Use [audit guidance](references/audit.md).
  Inspect pinned revisions and available validation evidence. Report findings
  and gaps; fresh checks follow the audit's scope. Auditing does not invoke
  rebase or publication.
- **Prepare or rebase a release:** Use [preparation](references/preparation.md)
  and [audit guidance](references/audit.md). Complete the exact-candidate build
  and validation gates, resolve migration regressions, and review the result.
- **Publish:** Use [publication](references/publication.md) after the candidate
  passes those gates and the session authorizes publication. A request to
  make/publish a release includes its branch push, new tip tag, and GitHub
  release; an audit or planning request does not.

Read only the references needed for the requested workflow. Preserve unrelated
files, worktrees, and release lanes. Never silently move a published tag or
change published release metadata. A mainnet promotion carries qualified
testnet application code; changes require testnet qualification first.

For release preparation/publication, use independent review when available.
Give the reviewer the exact candidate, pinned revisions, and validation evidence;
keep it read-only on canonical refs and GitHub. Resolve findings and re-review
affected changes until there are no actionable findings.
Convergence requires the same final `new_tip`, every deviation accounted for,
and required checks passing. Disclose when independent review is unavailable;
the user may override delegation.
An audit completes with findings and evidence. Preparation completes with a
reviewed, validated local candidate. Publication completes with verification of
the authorized branch, tag, release metadata, and dependency reachability.
