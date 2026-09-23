---
name: firedancer-release-rebase
description: "Rebase FireBAM release branches onto official firedancer-io/firedancer releases, audit conflicts and preserved changes, verify fresh nproc-parallel builds, and publish branch-tip tags and GitHub releases using the existing release format. Use for versioned upstream release upgrades or audits; not upstream main migrations."
---

# Firedancer Release Rebase

Produce a reviewed FireBAM release from the exact requested upstream release while preserving the branch's intentional local behavior. A clean rebase and successful build are evidence, not a correctness verdict.

Follow the user's scope and existing authorization. A request to make/publish a release authorizes its branch push, new tip tag, and GitHub release. An audit request does not authorize history or GitHub changes; pin and inspect the existing revisions. A planning or skill-maintenance request does not authorize a real release. Never silently retag a published version or change another release lane.

## Pin inputs and isolate the work

- Read applicable `AGENTS.md` files. Treat live `src/` as authoritative; consult `bam_spec.md`, the BAM schema gitlink, and sibling BAM/reference-validator code when relevant.
- Inspect status, worktrees, remotes, branch, disk, memory, toolchains, and submodules. Use a dedicated candidate branch/worktree and build directory. Create a backup ref and preserve unrelated files and worktrees; never clean, reset, or stash user work.
- Resolve the official `firedancer-io/firedancer` release and canonical destination repository from live URLs/API responses. Inspect release notes, version files, commits, code, and submodule changes. Do not substitute upstream `main` or a globally latest release.
- Pin full hashes for `old_base` (previous official release in this lane), `old_tip` (destination branch before rebase), `new_base` (requested official release), and later `new_tip` (validated candidate). Record the remote branch tip separately and reconcile local/remote divergence. Determine `old_base` from release history and ancestry, not merely `merge-base`.
- Preserve the old cached upstream-tag hash, then fetch each official tag without touching identically named FireBAM tags:

```bash
git fetch --no-tags --refmap= --recurse-submodules=no upstream \
  "refs/tags/$upstream_tag:refs/upstream-releases/$upstream_tag" || exit 1
new_base=$(git rev-parse --verify "refs/upstream-releases/$upstream_tag^{commit}") || exit 1
```

The empty `--refmap=` prevents configured remote fetch mappings from overwriting fork tags. Verify the remote identity, peel annotated tags, compare the commit with the official repository, and stop on unexpected tag movement.

- Inspect the prior destination release in the same lane, or the user's named template. Save its title, body, tag convention, draft/prerelease/latest flags, and asset convention. Do not infer mainnet/testnet from memory.

## Respect the monthly release lanes

Firedancer uses monthly `YY.MM` release lines rather than a parallel legacy versioning track. The expected cadence is approximate: branch cut and stabilization on the first Tuesday, testnet tag on the second Tuesday, and mainnet tag on the fourth Tuesday. Treat those dates as planning context, not permission to invent a release or a reason to reject an explicitly requested out-of-cycle release.

Testnet and mainnet are separate promotion lanes within the month. Determine the lane from official release metadata and live history, not the patch suffix. A mainnet tag can be essentially identical to its qualified testnet tag; still create the distinct requested tag/release. Never add application changes while promoting testnet to mainnet. If any application change is needed before mainnet is published, it must appear in a testnet build first. Before a mainnet promotion, compare both the official and FireBAM candidates with the qualified testnet tags and account for every difference; allow only version/release metadata and the live binary-finalization steps. Stop on an unqualified code change.

Rerun the repository's current binary-finalization procedure for mainnet promotion and record its inputs and output. Today, `contrib/tag-release-firedancer.py` and `contrib/tag-release-frankendancer.py` call `contrib/geoip_db.py` to refresh the embedded `src/disco/gui/dbip.bin.zst`; future tooling may add steps, so inspect the live scripts rather than hardcoding GeoIP as the complete list. For a downstream FireBAM sync, the official tag remains authoritative: verify its finalization delta and do not generate an extra database or commit absent from that tag unless the FireBAM release procedure explicitly requires it.

Feature activation timing is not tied to an Agave compatibility calendar. Activate a feature only when both clients support it, and verify both implementations when a release changes activation behavior. Use the pinned Agave revision for source/ABI/version checks, not as a release-scheduling signal.

## Rebase and resolve with code context

For established linear release history, run this in the clean isolated candidate:

```bash
git rebase --no-update-refs --empty=stop --onto "$new_base" "$old_base"
```

Keep `--no-update-refs` explicit because inherited `rebase.updateRefs=true` can rewrite release and backup refs. Check protected refs afterward. Inspect existing merge commits before deciding how to preserve them; do not merge upstream or squash FireBAM history merely to avoid conflicts.

For every conflict, read `git rebase --show-current-patch`, unresolved paths, all index stages, upstream history, callers, and tests. During rebase, “ours” is the new base plus replayed work. Explain both intents and implement their combined behavior. Review adjacent cleanly applied hunks, APIs, topology wiring, generated definitions, layouts, and ABIs. Avoid whole-file side selection unless the other behavior is proved redundant.

Retain existing and incoming regression cases. For test-call-list conflicts, preserve both sets of calls and required fixture/workspace resets. Adapt changed APIs without inventing enum values or deleting assertions. Record every manual resolution, skipped/empty commit, and already-upstream drop, proving equivalent final behavior rather than relying on commit counts.

Derive changed submodule revisions from the four pinned parent trees. If Agave has local commits to migrate, audit those and the C/Rust boundary separately, publish the necessary dependency commit first within authorization, and prove it is fetchable through the final `.gitmodules` URL. A reachable ancestor need not be a branch tip. Do not migrate unchanged gitlinks speculatively.

Keep necessary fixes small and separate after the rebase. Identify whether each fixes upstream, compatibility, or a pre-existing defect. Fix the real contract instead of suppressing warnings, weakening assertions, or hiding a production defect in one fixture. Prior examples are diagnostic leads, not automatic patches: release-specific parser events, advertised-versus-internal store alignment, and GCC 12 strict-aliasing in both pack tests and readiness benchmarks.

## Audit every change

Run the bundled helper from any location inside the candidate repository:

```bash
python3 /path/to/firedancer-release-rebase/scripts/audit_trees.py \
  --repo "$candidate_worktree" --base "$old_base" --old "$old_tip" \
  --upstream "$new_base" --new "$new_tip" > "$audit_dir/tree-audit.json"
```

Exit 0 means no non-overlap deviation; shared paths still need semantic review. Exit 2 means deviations require explanation, including intentional fixes. Exit 1 means invalid inputs/history or execution failure. The helper compares full-tree blobs, modes, types, deletions, additions, and gitlinks; it does not inspect submodule internals or prove correctness.

Read the complete `git range-diff "$old_base..$old_tip" "$new_base..$new_tip"`. Account for every modified, missing, extra, renamed, generated, or shared path. Expected non-overlap content is the new upstream tree for upstream-only changes and the previous FireBAM tree otherwise. Patch IDs support unchanged-delta claims but cannot prove semantics. Prove already-upstream behavior in the final tree.

Review interactions even without conflicts. Trace affected behavior through config, both validator topologies, links, verification/keyguard, pack, execution, PoH/replay, and BAM feedback as applicable. Recheck changed BAM limits against live node code and preserve ownership handshakes, durable results, leader timing, and mode transitions.

Record pinned hashes, commit mapping, conflicts, exceptions, submodule evidence, commands/results, and gaps. Unexplained behavior, hunks, or production changes keep the audit open. Require clean tracked candidate content and initialized reviewed gitlinks; never stage build products, configs, temporary patches, or logs.

## Build the exact candidate

Inspect live build configuration and CI. Verify compilers actually compile, dependencies/toolchains, disk, memory/cgroups, and CPU availability. Record `nproc`, compiler/linker versions, `MACHINE`, `EXTRAS`, `CC`, `CXX`, `MAKEFLAGS`, and Cargo job settings. Use fresh candidate/compiler-specific disk-backed output; do not clean shared symlinked outputs. Initialize committed submodules and ensure build metadata embeds `new_tip`.

Run the final default-toolchain build with the user's required concurrency. Define absolute `release_log_dir` and full `new_tip`; capture output directly so `tee` cannot mask failures:

```bash
release_jobs=$(nproc)
test "$release_jobs" -gt 0 || exit 1
test "$(git rev-parse HEAD)" = "$new_tip" || exit 1
git diff --quiet --ignore-submodules=none "$new_tip" -- || exit 1
mkdir -p "$release_log_dir"
git rev-parse HEAD > "$release_log_dir/commit.txt"
printf '%s\n' "$release_jobs" > "$release_log_dir/jobs.txt"

# Fresh default target, default toolchain, all available CPU jobs.
if make -j"$release_jobs" > "$release_log_dir/default-build.log" 2>&1; then
  printf '0\n' > "$release_log_dir/default-build.exit"
else
  release_status=$?
  printf '%s\n' "$release_status" > "$release_log_dir/default-build.exit"
  exit "$release_status"
fi

# Inspect live targets/products first; these are the usual two validators.
if make -j"$release_jobs" firedancer fdctl > "$release_log_dir/validators-build.log" 2>&1; then
  printf '0\n' > "$release_log_dir/validators-build.exit"
else
  release_status=$?
  printf '%s\n' "$release_status" > "$release_log_dir/validators-build.exit"
  exit "$release_status"
fi

test "$(git rev-parse HEAD)" = "$new_tip" || exit 1
git diff --quiet --ignore-submodules=none "$new_tip" -- || exit 1
```

A serial/`-j4`, compiler override, incremental no-op, or successful retry after a failed `-jN` build does not satisfy this gate. Diagnose dependency races, compiler errors, or resource limits; re-run a fresh `-jN` build after fixes. Do not suppress `-Werror` or silently reduce jobs. Inspect logs for real compiler/linker work and confirm expected products; default `all` excludes Rust `fdctl` and conditional makefiles can omit `firedancer`.

Run both binaries' `--version`. Derive Firedancer's expected version from its version file and fdctl's independent Frankendancer/Agave version from its own files and pinned Agave; never force fdctl to match `v26.*`. Require both to identify the final candidate commit. Sequentially build with a second supported compiler when available, especially GCC 12 for historical strict-aliasing diagnostics.

Build affected tests before running them. Include incoming/conflict/extra-fix regressions plus affected BAM, pack/readiness, resolver, execution, PoH/replay, snapshot, feature, config, and both topology paths. Raise memlock in the same shell and follow `AGENTS.md`; use supported normal pages and serialize large workspace tests when necessary. Preserve failed attempts and distinguish infrastructure with evidence. Use focused negative controls for adapted regressions or real production fixes. State integration/live-validator gaps accurately.

Any later source, gitlink, or build-configuration change invalidates affected results. Rebuild and refresh the audit on the final committed state.

## Review to convergence

Use an independent subagent when available for a real release workflow. Give it pinned revisions, candidate, instructions, and evidence; keep it read-only on canonical refs and GitHub. Resolve each finding in code or with specific evidence, refresh validation, and request review of the new exact candidate. Convergence means no actionable findings, every deviation accounted for, required checks passing, and both agents reviewing the same `new_tip`. Investigate disagreements rather than declaring convergence; disclose if delegation is unavailable. The user may override delegation.

## Publish and verify

Publish only when the exact final candidate passed the gates and session authorization covers publication. Prepare and review title/body/flags before publishing; titles, descriptions, and release notes are immutable after publication. Resolve the canonical repository and preserve the same-network template and asset convention. Testnet notes and changelogs compare with the previous testnet release; mainnet notes and changelogs compare with the previous mainnet release, even when the immediately preceding tag belongs to the other lane or the testnet/mainnet source commits are identical. A common body, only when live history confirms it, is:

```text
Sync with upstream https://github.com/firedancer-io/firedancer/releases/tag/<new-version>

**Full Changelog**: https://github.com/<canonical-fork>/compare/<previous-lane-tag>...<new-version>
```

Match mainnet/testnet wording and explicitly set prerelease/latest behavior; do not let an older/testnet lane replace stable latest. Do not attach native-machine binaries when the convention is source archives only. If published metadata is wrong, do not edit it in place; stop and follow the repository's correction-release procedure.

Immediately before push, require HEAD and clean tracked content to match `new_tip`; recheck the official tag still peels to `new_base`. Re-read the destination branch. If it equals `old_remote_tip`, use the original exact lease. If it equals `new_tip`, verify a prior push succeeded and resume missing steps. Any other value requires reconciliation and revalidation. Never merely update the lease expectation.

Check local/remote target tags and the GitHub release. Do not move an existing published tag. If a new lightweight tag matches repository convention:

```bash
git tag "$release_tag" "$new_tip" || exit 1
test "$(git rev-parse --verify "refs/tags/$release_tag^{commit}")" = "$new_tip" || exit 1
git push --atomic --no-follow-tags \
  --force-with-lease="refs/heads/$release_branch:$old_remote_tip" \
  "$destination_remote" \
  "$new_tip:refs/heads/$release_branch" \
  "refs/tags/$release_tag:refs/tags/$release_tag" || exit 1
```

`--no-follow-tags` prevents inherited `push.followTags=true` from publishing unrelated annotated tags. Never force-update an existing release tag. If atomic push is unsupported, verify state and use ordered branch-then-tag pushes with equivalent leases, checks, and `--no-follow-tags`. On any network/API failure, query refs and release state before retrying; resume idempotently.

Create the release against the explicit repository and existing verified tag. With `gh release create`, use `--verify-tag` and `--notes-file`; with REST, verify the remote tag first and send an exact JSON file. Set title, body, prerelease/latest flags, and assets deliberately.

Finally verify destination branch and peeled tag both equal `new_tip`; release title/body/flags/assets are exact; dependency gitlinks are fetchable; and candidate tracked content remains clean. Reconcile local branches safely without force-moving another worktree. For multiple lanes, track and validate each independently.

Report the release link, final commit, upstream base, conflict resolutions/extras, actual build/test coverage, and limitations. Do not imply selected tests prove live validator operation.
