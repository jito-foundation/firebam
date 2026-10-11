#!/usr/bin/env python3
"""Compare four release trees without modifying the repository.

Exit 0: no non-overlap deviations (shared-path review may remain).
Exit 2: non-overlap deviations requiring explanation. Exit 1: invalid inputs.
This is evidence, never a semantic correctness or publication verdict.
"""

import argparse
import json
import subprocess
import sys


def git(repo, *args):
    return subprocess.check_output(["git", "-C", repo, *args], stderr=subprocess.PIPE)


def commit(repo, ref):
    return git(repo, "rev-parse", "--verify", "--end-of-options", ref + "^{commit}").decode().strip()


def ancestor(repo, first, second):
    result = subprocess.run(
        ["git", "-C", repo, "merge-base", "--is-ancestor", first, second],
        capture_output=True,
    )
    if result.returncode not in (0, 1):
        raise ValueError(result.stderr.decode(errors="replace"))
    return result.returncode == 0


def tree(repo, rev):
    result = {}
    for record in git(repo, "ls-tree", "--full-tree", "-r", "-z", rev).split(b"\0"):
        if not record:
            continue
        metadata, path = record.split(b"\t", 1)
        mode, kind, oid = metadata.decode().split()
        result[path.decode("utf-8", "surrogateescape")] = {
            "mode": mode, "type": kind, "oid": oid,
        }
    return result


def audit(repo, refs):
    revisions = {name: commit(repo, ref) for name, ref in refs.items()}
    for first, second in (("base", "old"), ("base", "upstream"), ("upstream", "new")):
        if not ancestor(repo, revisions[first], revisions[second]):
            raise ValueError(f"{first} is not an ancestor of {second}; reconcile release boundaries")
    trees = {name: tree(repo, rev) for name, rev in revisions.items()}
    paths = set().union(*(set(t) for t in trees.values()))
    incoming = {p for p in paths if trees["base"].get(p) != trees["upstream"].get(p)}
    local = {p for p in paths if trees["base"].get(p) != trees["old"].get(p)}
    overlaps, deviations = [], []
    matched = 0
    for path in sorted(paths):
        values = {name: t.get(path) for name, t in trees.items()}
        if path in incoming and path in local and values["upstream"] != values["old"]:
            overlaps.append({"path": path, **values})
            continue
        expected = values["upstream"] if path in incoming else values["old"]
        if values["new"] != expected:
            deviations.append({"path": path, "expected": expected, **values})
        else:
            matched += 1
    submodules = [
        {"path": p, **{name: t.get(p) for name, t in trees.items()}}
        for p in sorted(paths)
        if any(t.get(p, {}).get("mode") == "160000" for t in trees.values())
    ]
    counts = {}
    for name, first, second in (("old_local", "base", "old"), ("new_local", "upstream", "new")):
        span = revisions[first] + ".." + revisions[second]
        counts[name] = int(git(repo, "rev-list", "--count", span))
        counts[name + "_merges"] = int(git(repo, "rev-list", "--count", "--merges", span))
    return {
        "schema_version": 1,
        "revisions": revisions,
        "verdict": "mechanical evidence only; semantic review and validation required",
        "tracked_paths": len(paths),
        "matched_nonoverlap_paths": matched,
        "upstream_changed_paths": sorted(incoming),
        "previous_firebam_changed_paths": sorted(local),
        "shared_paths_requiring_review": overlaps,
        "nonoverlap_deviations_requiring_explanation": deviations,
        "submodule_entries": submodules,
        "commit_counts": counts,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    for name in ("base", "old", "upstream", "new"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    try:
        result = audit(args.repo, {name: getattr(args, name) for name in ("base", "old", "upstream", "new")})
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        detail = error.stderr.decode(errors="replace") if isinstance(error, subprocess.CalledProcessError) else str(error)
        print(json.dumps({"error": detail}), file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, ensure_ascii=True))
    return 2 if result["nonoverlap_deviations_requiring_explanation"] else 0


if __name__ == "__main__":
    sys.exit(main())
