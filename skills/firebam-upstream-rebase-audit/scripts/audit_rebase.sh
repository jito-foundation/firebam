#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <old-base> <old-tip> <new-base> <new-tip> [repository]" >&2
  exit 2
}

[[ $# -ge 4 && $# -le 5 ]] || usage

old_base=$1
old_tip=$2
new_base=$3
new_tip=$4
repo=${5:-.}

git_cmd=(git -C "$repo")

for rev in "$old_base" "$old_tip" "$new_base" "$new_tip"; do
  "${git_cmd[@]}" cat-file -e "${rev}^{commit}" 2>/dev/null || {
    echo "error: $rev is not a commit in $repo" >&2
    exit 2
  }
done

heading() {
  printf '\n== %s ==\n' "$1"
}

count_status() {
  local status=$1
  local base=$2
  local tip=$3
  "${git_cmd[@]}" diff --diff-filter="$status" --name-only "$base..$tip" | wc -l | tr -d ' '
}

heading "Pinned revisions"
for item in \
  "old-base:$old_base" \
  "old-tip:$old_tip" \
  "new-base:$new_base" \
  "new-tip:$new_tip"; do
  label=${item%%:*}
  rev=${item#*:}
  printf '%-9s ' "$label"
  "${git_cmd[@]}" show -s --format='%H %ci %s' "$rev"
done

heading "History shape"
printf 'old commit count: '
"${git_cmd[@]}" rev-list --count "$old_base..$old_tip"
printf 'new commit count: '
"${git_cmd[@]}" rev-list --count "$new_base..$new_tip"
printf 'new merge count:  '
"${git_cmd[@]}" rev-list --count --merges "$new_base..$new_tip"
if "${git_cmd[@]}" merge-base --is-ancestor "$new_base" "$new_tip"; then
  echo "new base ancestry: yes"
else
  echo "new base ancestry: NO"
fi

heading "Delta status counts"
printf '%-6s %7s %7s %7s %7s %7s %7s\n' range A M D R C T
for range_name in old new; do
  if [[ $range_name == old ]]; then
    base=$old_base
    tip=$old_tip
  else
    base=$new_base
    tip=$new_tip
  fi
  printf '%-6s %7s %7s %7s %7s %7s %7s\n' \
    "$range_name" \
    "$(count_status A "$base" "$tip")" \
    "$(count_status M "$base" "$tip")" \
    "$(count_status D "$base" "$tip")" \
    "$(count_status R "$base" "$tip")" \
    "$(count_status C "$base" "$tip")" \
    "$(count_status T "$base" "$tip")"
done

heading "Delta path-set differences"
echo "old-only paths:"
comm -23 \
  <("${git_cmd[@]}" diff --name-only "$old_base..$old_tip" | sort) \
  <("${git_cmd[@]}" diff --name-only "$new_base..$new_tip" | sort)
echo "new-only paths:"
comm -13 \
  <("${git_cmd[@]}" diff --name-only "$old_base..$old_tip" | sort) \
  <("${git_cmd[@]}" diff --name-only "$new_base..$new_tip" | sort)

heading "Added/deleted path-set differences"
echo "added in only one delta:"
comm -3 \
  <("${git_cmd[@]}" diff --diff-filter=A --name-only "$old_base..$old_tip" | sort) \
  <("${git_cmd[@]}" diff --diff-filter=A --name-only "$new_base..$new_tip" | sort)
echo "deleted in only one delta:"
comm -3 \
  <("${git_cmd[@]}" diff --diff-filter=D --name-only "$old_base..$old_tip" | sort) \
  <("${git_cmd[@]}" diff --diff-filter=D --name-only "$new_base..$new_tip" | sort)

heading "Added files whose final blob or mode changed"
different_added=0
while IFS= read -r file_name; do
  old_entry=$("${git_cmd[@]}" ls-tree "$old_tip" -- "$file_name")
  new_entry=$("${git_cmd[@]}" ls-tree "$new_tip" -- "$file_name")
  if [[ $old_entry != "$new_entry" ]]; then
    different_added=$((different_added+1))
    printf '%s\n  old %s\n  new %s\n' "$file_name" "$old_entry" "$new_entry"
  fi
done < <("${git_cmd[@]}" diff --diff-filter=A --name-only "$old_base..$old_tip")
echo "different added files: $different_added"

heading "Paths changed by both FireBAM and upstream"
mapfile -t overlap_paths < <(
  comm -12 \
    <("${git_cmd[@]}" diff --name-only "$old_base..$old_tip" | sort) \
    <("${git_cmd[@]}" diff --name-only "$old_base..$new_base" | sort)
)
printf '%s\n' "${overlap_paths[@]:-}"

heading "Upstream commits touching overlap paths"
if (( ${#overlap_paths[@]} )); then
  "${git_cmd[@]}" log --oneline --no-merges "$old_base..$new_base" -- "${overlap_paths[@]}"
else
  echo "none"
fi

heading "Range diff"
"${git_cmd[@]}" range-diff --no-color "$old_base..$old_tip" "$new_base..$new_tip"

heading "Whitespace and conflict-marker checks"
"${git_cmd[@]}" diff --check "$new_base..$new_tip"
if "${git_cmd[@]}" grep -n -E '^(<<<<<<< |>>>>>>> |\|\|\|\|\|\|\| )' "$new_tip" --; then
  echo "warning: conflict-marker-like text exists at the new tip" >&2
else
  echo "no conflict markers found"
fi

heading "Submodule delta"
"${git_cmd[@]}" diff --submodule=log "$new_base..$new_tip" -- .gitmodules || true
mapfile -t submodule_paths < <(
  "${git_cmd[@]}" ls-tree -r "$new_tip" | awk '$1=="160000" { print $4 }'
)
if (( ${#submodule_paths[@]} )); then
  "${git_cmd[@]}" diff --submodule=log "$new_base..$new_tip" -- "${submodule_paths[@]}" || true
else
  echo "no submodules at new tip"
fi

heading "Reminder"
echo "Mechanical checks complete. Review every range-diff deviation and trace"
echo "upstream-added predicates, initialization, and security boundaries before"
echo "declaring the rebase correct."
