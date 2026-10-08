# Publish a validated FireBAM release

Use this phase only with publication authorization and evidence that the exact
candidate passed [preparation](preparation.md#build-the-exact-candidate) and
[audit](audit.md). Preserve prior release metadata and published tags.
Publish necessary changed dependency commits first within authorization and
verify the final gitlinks are fetchable before publishing the parent.

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
