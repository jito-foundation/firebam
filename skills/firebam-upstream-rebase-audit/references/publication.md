# Publish an upstream-main candidate

Use this only when the session authorizes publication, the exact candidate has
passed its audit and affected checks, and changed dependency commits are
fetchable through the final `.gitmodules` URL. Canonical branch replacement
must use the pinned remote tip as its lease.

Before publication, fetch `origin/main` again and require it to equal
`old_tip`. Publish only with the exact lease:

```bash
git fetch origin main
test "$(git rev-parse origin/main)" = "$old_tip"
git push --force-with-lease="refs/heads/main:$old_tip" \
  origin "$new_tip:main"
```

If the equality check or lease fails, stop and reconcile the new remote work.
