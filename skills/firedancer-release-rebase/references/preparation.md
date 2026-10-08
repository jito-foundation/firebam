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

Rebase in a separate worktree, with a backup ref of the lane tip:

```bash
git rebase --no-update-refs --empty=stop --onto "$new_base" "$old_base"
```

Keep `--no-update-refs` explicit: an inherited `rebase.updateRefs=true` rewrites
release and backup refs. Don't merge upstream or squash FireBAM history to dodge
conflicts. In test-list conflicts keep both sides' cases. Derive gitlinks from
the four pinned parent trees and migrate only the ones that changed.

Mainnet promotion reruns the repository's binary finalization. Today
`contrib/tag-release-firedancer.py` and `contrib/tag-release-frankendancer.py`
refresh `src/disco/gui/dbip.bin.zst` through `contrib/geoip_db.py`; read the
live scripts, since steps may be added. For a downstream sync the official tag's
finalization is authoritative: add no database or commit the tag doesn't have.

Build gate, on the exact committed `new_tip` with a clean tree:

- A fresh `make -j"$(nproc)"`, then `make -j"$(nproc)" firedancer fdctl`
  (`all` omits the Rust-linked `fdctl`), with logs and exit codes kept. A
  serial build, an incremental no-op, or a retry after a failed parallel build
  does not count. Fix races instead of lowering jobs or `-Werror`.
- `--version` of both binaries names `new_tip`. Firedancer's version comes from
  its version file. `fdctl` reports its own Frankendancer/Agave version (for
  example `0.101.0-beta.40302`) and never matches `v26.*`.
- A second compiler when available, especially GCC 12 for strict-aliasing
  diagnostics.
- The affected tests, built first.

Any later source, gitlink, or build-configuration change invalidates the gate.
