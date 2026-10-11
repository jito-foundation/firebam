# Publish main

`main` requires pull requests and blocks force-pushes, and every branch
requires signed commits. Replacing `main` with a rebase therefore needs an
admin ruleset bypass. Never re-sign upstream commits to avoid the bypass: the
upstream history must stay intact.

Fetch again and require `origin/main` to equal `old_tip`, then push with that
exact lease:

```bash
git push --force-with-lease="refs/heads/main:$old_tip" origin "$new_tip:refs/heads/main"
```

If the check or the lease fails, stop and reconcile the new remote work. Do not
just update the lease.
