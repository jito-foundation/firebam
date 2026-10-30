# Prepare a release candidate

FireBAM and upstream use the same tag names, so never run
`git fetch upstream --tags`. Fetch the official tag into its own namespace:

```bash
git fetch --no-tags --refmap= --recurse-submodules=no upstream \
  "refs/tags/${upstream_tag}:refs/upstream-releases/${upstream_tag}"
new_base=$(git rev-parse --verify "refs/upstream-releases/${upstream_tag}^{commit}")
```

The empty `--refmap=` stops configured fetch mappings from overwriting fork
tags. Keep the braces, because zsh reads `$t:r` as a modifier. Compare the
peeled commit with the official repository and stop if the tag moved.

Rebase in a separate worktree, with a backup ref of the lane tip, using the
`git rebase --no-update-refs` command in
[the main-rebase migration](../../firebam-upstream-rebase-audit/references/migration.md).
Never merge the release into the lane, whether to dodge conflicts or to let a
pull request land without a force-push: the merge hides the sync work from the
audit's range-diff, and the next sync must resolve the same conflicts again.
Don't squash FireBAM history either. When the release renames or removes
something FireBAM uses, fold the fix into the earliest FireBAM commit that needs
it, so every commit builds. In test-list conflicts keep both sides' cases.
Derive gitlinks from the four pinned parent trees and migrate only the ones
that changed.

Mainnet promotion reruns the repository's binary finalization. Today
`contrib/tag-release-firedancer.py` and `contrib/tag-release-frankendancer.py`
refresh `src/disco/gui/dbip.bin.zst` through `contrib/geoip_db.py`; read the
live scripts, since steps may be added. For a downstream sync the official tag's
finalization is authoritative: add no database or commit the tag doesn't have.

Build gate, on the exact committed `new_tip` with a clean tree:

- A fresh `make -j"$(nproc)" all`, then `make -j"$(nproc)" firedancer fdctl`
  (`all` omits the Rust-linked `fdctl`), with logs and exit codes kept. Use a
  new `BUILDDIR` for each compiler configuration. A serial build, an
  incremental no-op, or a retry after a failed parallel build does not count.
  Fix races instead of lowering jobs or `-Werror`.
- `--version` of both binaries names `new_tip`. Firedancer's version comes from
  its version file. `fdctl` reports its own Frankendancer/Agave version (for
  example `0.101.0-beta.40302`) and never matches `v26.*`.
- Run `all` with GCC and repeat it with a second compiler when available,
  each in a fresh build directory. Prefer GCC 12 when available for
  strict-aliasing diagnostics.
- Build and run the affected tests. Building `all` does not execute them.

Any later source, gitlink, or build-configuration change invalidates the gate.
