# Audit a release candidate

Pin `old_base` (the lane's previous official release), `old_tip` (the lane
before the rebase), `new_base` (the requested release), and `new_tip` (the
candidate), then run:

```bash
python3 skills/firedancer-release-rebase/scripts/audit_trees.py \
  --repo "$candidate_worktree" --base "$old_base" --old "$old_tip" \
  --upstream "$new_base" --new "$new_tip" > tree-audit.json
```

Lanes don't carry this skill; take the script from `main` with
`git show origin/main:skills/firedancer-release-rebase/scripts/audit_trees.py`.

The script expects every path outside the overlap to hold the new upstream
blob for upstream-only changes and the old FireBAM blob otherwise. Exit 0 means
no deviation, exit 2 means deviations to explain (intentional fixes included),
and exit 1 means bad inputs. It compares trees and gitlinks only: paths both
sides changed still need semantic review, and submodule contents are audited
separately.

Read the full `git range-diff "$old_base..$old_tip" "$new_base..$new_tip"`, and
prove each already-upstream drop in the final tree. Review interactions as for
a main rebase, even where nothing conflicted. The audit ends with a verdict on
the exact `new_tip`; an unresolved finding fails it.
