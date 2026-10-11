# Publish a release

The canonical repository is `jito-foundation/firebam`. Lanes take
force-with-lease pushes. Every branch requires signed commits, so pushing the
unsigned official commits needs an admin bypass. Never re-sign them: the
official tag must stay an ancestor.

Copy the previous release in the same lane for the title (for example
"Firedancer Testnet vX"), flags, and tag style (lightweight tag, no binary
assets). The usual body:

```text
Sync with upstream https://github.com/firedancer-io/firedancer/releases/tag/<new-version>

**Full Changelog**: https://github.com/jito-foundation/firebam/compare/<previous-lane-tag>...<new-version>
```

The changelog compares with the previous release in the same lane, even when
the preceding tag belongs to the other lane. Testnet releases are
`--prerelease --latest=false`; never let a testnet or older lane take latest.
Release text is effectively immutable: get it right first, and fix a published
mistake with the correction-release procedure rather than an edit.

Right before pushing, check that the official tag still peels to `new_base`.
Push the branch and tag together, leased on the lane's pinned remote tip:

```bash
git tag "$release_tag" "$new_tip"
git push --atomic --no-follow-tags \
  --force-with-lease="refs/heads/$release_branch:$old_remote_tip" \
  origin "$new_tip:refs/heads/$release_branch" \
  "refs/tags/$release_tag:refs/tags/$release_tag"
```

`--no-follow-tags` stops an inherited `push.followTags` from publishing other
tags. Never move a published tag. If the branch already equals `new_tip`, an
earlier push landed: resume the missing steps. Any other value means reconcile
and revalidate, never a new lease.

Create the release with
`gh release create "$release_tag" --repo jito-foundation/firebam --verify-tag --notes-file <file>`
plus the lane's title and flags, then check that the branch and the peeled tag
equal `new_tip` and the release metadata is exact.
