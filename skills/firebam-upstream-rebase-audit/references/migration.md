# Rebase main onto upstream

Pin full hashes: `old_tip` (`origin/main`), `new_base` (`upstream/main`) and
`old_base` (their merge base). Keep a backup ref of `old_tip` and work in a
separate worktree.

Agave first. Read gitlinks from the pinned parent trees
(`git ls-tree "$rev" agave`), not from the checked-out submodule. If the old
tip's Agave equals the old base's, take the new base's Agave unchanged.
Otherwise rebase the FireBAM Agave commits onto the new base's Agave; before
the parent is published, that exact commit must be fetchable from the
`.gitmodules` URL.

Then the parent:

```bash
git rebase --no-update-refs --empty=stop --onto "$new_base" "$old_base"
```

Keep `--no-update-refs` explicit: an inherited `rebase.updateRefs=true` moves
backup and other branch refs. On conflicts, combine both intents rather than
taking a side. Record each manual resolution, and each hunk dropped because
upstream now has it together with the upstream commit that proves it.

Done when both validators build (`firedancer` and `fdctl`; `all` omits the
Rust-linked `fdctl`), the affected BAM and upstream tests pass, the stateful
BAM corpus replays clean if the pipeline changed, any growth in the upstream
delta is explained, and the [audit](audit.md) has no open finding.
