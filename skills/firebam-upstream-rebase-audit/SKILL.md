---
name: firebam-upstream-rebase-audit
description: Plan, perform, or audit FireBAM migrations onto Firedancer upstream main, including affected Agave changes. Use for main rebases and candidate audits; versioned releases use firedancer-release-rebase.
---

# FireBAM Upstream Rebase Audit

Preserve intentional FireBAM behavior when moving to upstream main. A successful
Git rebase begins verification; audit both the migrated delta and upstream code
that consumes or gates BAM behavior. Live `src/` is authoritative; consult the
local spec, schema, and sibling code when the changed behavior needs them.

## Select the workflow

- **Plan:** Identify the target, current branch/dependency revisions, affected
  boundaries, and proposed validation. Use procedure details only when needed
  to explain the plan; planning does not execute the migration.
- **Audit an existing candidate:** Use [audit guidance](references/audit.md).
  Inspect pinned parent/Agave revisions and existing validation evidence;
  report findings and coverage gaps without changing history or publishing.
- **Perform a main rebase:** Use [migration](references/migration.md), then
  [audit guidance](references/audit.md). Validate the exact final candidate,
  resolve regressions caused by the migration, and review the result.
- **Publish:** Use [publication](references/publication.md) only when the
  session authorizes publication and the candidate has passed its checks.

Read only the references needed for the requested workflow. A plan or audit
request does not authorize rebase or publication. Reversible candidate work
follows the user's scope; canonical branch changes and external review
artifacts require existing authorization. Use isolated worktrees when needed
and preserve unrelated work; never clean, reset, or stash user files.

Report the requested outcome with pinned revisions, preservation evidence,
validation results or gaps, and unresolved findings. An audit may complete with
findings; a release-ready correctness verdict requires those findings resolved.
