# Rebase main onto upstream

Pin full hashes: `old_tip` (`origin/main`), `new_base` (`upstream/main`) and
`old_base` (their merge base). Keep a backup ref of `old_tip` and work in a
separate worktree.

Rebase:

```bash
git rebase --no-update-refs --empty=stop --onto "$new_base" "$old_base"
```

Keep `--no-update-refs` explicit: an inherited `rebase.updateRefs=true` moves
backup and other branch refs. On conflicts, combine both intents rather than
taking a side. Record each manual resolution, and each hunk dropped because
upstream now has it together with the upstream commit that proves it.

Build gate, on the exact committed `new_tip` with a clean tree:

- A fresh `make -j"$(nproc)" all`, with logs and exit codes kept. Use a
  new `BUILDDIR` for each compiler configuration. A serial build, an
  incremental no-op, or a retry after a failed parallel build does not count.
  Fix races instead of lowering jobs or `-Werror`.
- `--version` of `firedancer` and `firedancer-dev` names `new_tip`.
- Run `all` with GCC and repeat it with a second compiler when available,
  each in a fresh build directory. Prefer GCC 12 when available for
  strict-aliasing diagnostics.
- Build and run the affected BAM and upstream tests. Replay the stateful BAM
  corpus if the pipeline changed. Building `all` does not execute the tests.

Any later source, gitlink, or build-configuration change invalidates the gate.
Done when the gate passes, any growth in the upstream delta is explained, and
the [audit](audit.md) has no open finding.
