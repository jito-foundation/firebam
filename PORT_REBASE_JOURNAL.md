# BAM v1.1 → main Port/Rebase Verification Journal

## Scope

- Source feature branch: `eric/v1.1-bam`
- Source feature commit: `6d0a4d36f8373823805d5e40d56a074309d8b233`
- Source baseline: `30902441f1fe990f994f4a2d961c82029cb214b2` (`v1.1.4`)
- Port target branch: `codex/rebase-5748e0c9-main`
- Port commit under audit: `ec265e2fdd5c4484c24589fc647e49c09f52150a`
- Pre-hardening verified head: `0da52d75a2977dd94bba90d691e050d53054b9ad`
- Target baseline: `45b9ece5217de3e3c058cfe02738fc5820dadabd` (`main` at port time)
- Audit started: 2026-08-10 (America/Los_Angeles)

The objective is stronger than “the cherry-pick completed”: prove that every
source BAM file and feature is either represented in the target commit or is
already supplied by the newer target baseline, that current-main adaptations
preserve the intended behavior, and that the resulting branch builds/tests.

## Status

**COMPLETE — production, test, fuzz, and submodule publication issues are
resolved in the validated follow-up.** Commit `0da52d75a2` contains the eight
current-main harness adaptations and reproducible Agave gitlink.  The
additional sanitizer hardening documented below is the follow-up delta on top
of that commit.

## Port lineage

The target is not a graph descendant of `eric/v1.1-bam`. Both branches contain
one squashed BAM feature commit, but on different baselines:

```text
30902441f1 (v1.1.4) ── 6d0a4d36f8 (source BAM)

45b9ece521 (main)   ── ec265e2fdd (ported BAM)
```

Reflog evidence for the target:

1. `45b9ece521` — branch created from `main`.
2. `e31640e9fe` — cherry-pick of the squashed BAM commit.
3. `5f82da4fa0` — amend with current-main source/test adaptations.
4. `ec265e2fdd` — amend correcting the Agave gitlink.
5. Follow-up corrective commit — audit journal, eight harness fixes, and
   reachable Agave submodule routing.

## Evidence collected

### Pre-hardening branch-head re-verification (2026-08-10)

Commit `0da52d75a2` was re-audited independently after the corrective commit:

- All 305 source paths are accounted for: 302 are shared with the target patch,
  the two source-only gRPC files are byte-identical to the target baseline, and
  the source-only URL validation is already present in the stricter target
  baseline. `PORT_REBASE_JOURNAL.md` is the sole target-only path. There are
  zero file-status or mode mismatches across the 302 shared paths.
- All 20 focused GCC/`-Werror` binaries rebuilt and passed in a single fresh
  execution sweep: the BAM tile/admin, Pack/tile, dedup, bundle, execle, PoH,
  verify, topology, resolver, GUI, gRPC, URL, config, and genesis tests listed
  below.
- Both Clang/libFuzzer corpus gates rebuilt and passed: all 26 BAM-client inputs
  and all 16 stateful-pipeline inputs ran ten times each.
- `git diff --check` passed. Syntax/parse checks passed for all 20 changed shell
  scripts, 10 Python files, 2 JSON files, 37 TOML files, and 5 YAML files. A
  metrics regeneration produced 1,763 metrics for 47 tiles with no tracked
  output change, and both Rust utility manifests passed `cargo metadata`.
- The Agave four-commit `range-diff`, aggregate stable patch ID, seven changed
  paths, and all seven per-file stable patch IDs were reproduced. The configured
  Agave branch still advertises exact tip `e71292b417...`; a clean temporary
  repository also fetched exact BAM protobuf pin `56973b61e8...` from its
  configured public remote.
- The native `firedancer` executable relinked at branch head and reports
  `0da52d75a2...`. A fresh combined `fdctl` invocation again exhausted the
  `/tmp` quota while Cargo wrote Agave metadata; it emitted no source diagnostic.
  The already-linked `fdctl` reports `ec265e2fdd...`, and the branch-head delta
  after that production snapshot contains only the submodule routing change,
  journal, and test/fuzz harness changes. The earlier successful combined link
  therefore remains valid production-link evidence for branch head.

### Deep sanitizer and invariant validation (2026-08-10)

A second correctness pass used a combined Clang AddressSanitizer and
UndefinedBehaviorSanitizer build, extended libFuzzer exploration, Clang's
static analyzer, and a fresh GCC rebuild.  It found and corrected three
production undefined-behavior defects:

- Ed25519 scalar reduction and multiply-add stored 64-bit output words through
  potentially unaligned `uchar *` buffers.  The stores now use `FD_STORE`.
- Gossip pull-request Bloom filters initialized `ulong` arrays directly inside
  inherently unaligned serialized bincode fields.  The filter is now built in
  aligned local storage and copied into the wire buffer; the serialized output
  API now exposes byte pointers so consumers must use unaligned-safe copies or
  stores, and the serialized `bits_set` field uses `FD_STORE`.
- The empty gossip-purged iterator formed an out-of-range pool pointer before
  testing its null index.  It now checks the sentinel first.

The same pass corrected three validation defects: the stateful fuzzer no
longer expects a duplicate terminal result for a stale replay, the Pack tile
test now constructs a real dcache (including its private header), and the Pack
nonce test uses a large finite priority that does not overflow through
`pow(5, priority)`.

Post-fix execution evidence:

- The combined-sanitizer BAM-client fuzzer completed 968,128 generated inputs
  in 121 seconds without a crash or sanitizer report.
- The combined-sanitizer gossip verifier fuzzer completed 15,193 generated
  inputs in 31 seconds after the serialized-output API correction.
- The stateful pipeline fuzzer first exposed the stale-replay model defect on a
  reproducible 61-byte trace.  After the harness correction, that exact trace,
  all 16 tracked corpus inputs (ten runs each with the required 3 GiB RSS cap),
  and a further 121-second/1,799-input exploratory run all passed.
- Eighteen focused unit binaries passed with both sanitizers, including BAM,
  Pack, bundle, execle, PoH, verify, resolver, gRPC, URL, Bloom, and Ed25519
  coverage.  The large BAM tile fixture required a 128 MiB test-process stack;
  this is sanitizer fixture overhead, not production stack usage.
- Twenty-four focused GCC/`-Werror` binaries rebuilt and passed, adding the two
  Ed25519 tests, Bloom test, and gossip weighted-sampler test to the original
  20-test sweep.
- Clang's static analyzer reported no findings in ten selected BAM, Pack,
  gossip, and Ed25519 translation units.
- Both `firedancer` and the combined `fdctl` relinked from the corrected
  objects and executed `--version`.  The latter completed a fresh
  `agave-validator` release-with-debug build in 12m07s and successfully copied
  its static archive, so the earlier `/tmp` capacity limitation did not recur.
  `git diff --check` passed.

### Changed-path inventory

| Check | Result |
|---|---:|
| Source commit paths | 305 |
| Initial/final port-commit paths | 302 |
| Branch-head cumulative paths | 303 |
| Shared source/branch-head paths | 302 |
| Source-only paths | 3 |
| Branch-head-only paths | 1 (`PORT_REBASE_JOURNAL.md`) |

Source-only paths requiring baseline-equivalence review:

- `src/waltz/grpc/fd_grpc_client.c`
- `src/waltz/grpc/fd_grpc_client.h`
- `src/waltz/http/fd_url.c`

No file that still needed a BAM-specific delta was intentionally added only on
the target side. The three omissions are fully accounted for by newer-main
commits:

- `4eab2518be` (`waltz: harden gRPC stream send state`) contains the complete
  source deltas to `fd_grpc_client.c` and `fd_grpc_client.h`, plus the associated
  tests. The source-tip and target-baseline blobs are identical for both files.
- `815ea59b26` (`waltz: reject malformed URL ports`) contains the source
  `fd_url.c` validation change plus stricter authority parsing and a larger test
  matrix. The source-added decimal-only port behavior is present unchanged.

Therefore these paths are baseline-supplied, not missing from the port.

### Shared-path content comparison (source vs. port commit)

All 302 shared changed paths retain the same add/modify status and file mode.

| Comparison | Exact | Needs semantic review |
|---|---:|---:|
| Resulting blob | 236 | 66 |
| Per-file stable patch ID | 267 | 35 |

The 31 paths with different final blobs but equal patch IDs are explained by
unrelated newer-baseline content around an unchanged BAM delta. The remaining
35 paths form the manual semantic-review surface.

### Post-cherry-pick adaptations

Nine files changed between the initial cherry-pick and first amend:

- `src/app/firedancer/test_topology_bam.c`
- `src/app/firedancer/topology.c`
- `src/disco/bam/test_bam_tile.c`
- `src/disco/gui/fd_gui.c`
- `src/disco/gui/test_gui_waterfall.c`
- `src/disco/pack/fd_pack.c`
- `src/disco/pack/test_pack.c`
- `src/disco/pack/test_pack_tile_bam.c`
- `src/discof/gossip/fd_gossip_tile.c`

The final amend changes only the `agave` gitlink. Every adaptation was reviewed
semantically and covered by a focused test where one exists.

The 35 non-identical per-file patches and the complete `git range-diff` were
reviewed. No production BAM hunk was lost. The meaningful current-main
adaptations are:

- Firedancer topology keeps current-main snapshot/rserve wiring and current
  identity handling while adding the BAM topology; DNS resolution uses the
  current `fd_dns_resolve_address` API. Its topology test supplies the newer
  backup/snapshot callbacks.
- The BAM tile test workspace was enlarged for current tile footprints.
- GUI code/tests preserve current-main transaction-v1 layout and APIs while
  retaining the BAM waterfall additions.
- Pack lookup uses the current map key structure. The exact malformed-D18
  regression is now split at the current parser boundary: the tile test proves
  that the exact packet is rejected, then the verify/resolver/Pack/BAM tests
  prove preservation and exact serialization of its failure index/reason.
- The gossip test shim is adapted to the current-main tile interface.

The other reviewed differences preserve current-main generated metrics,
configuration, genesis, admin, plugin, gossip, and transaction-v1 changes
around the unchanged BAM deltas. This closes the semantic-review surface.

### Submodule pins

`src/disco/bam/proto/bam-protos` is identical on source and target:
`56973b61e851a363019e8e0ac62e2634dd33bad7`.

The Agave baselines differ, so the root gitlink hashes correctly differ:

| Stack | Baseline | Tip |
|---|---|---|
| Source | `f69787f493` | `91c0cc47ac` |
| Target | `9d7e3c77ee` | `e71292b417` |

Both are four-commit stacks with the same subjects, in the same order:

1. `bam: report fee-payer balance and loaded account data size`
2. `bam: expose transaction batch failure policy through bank FFI`
3. `bam: publish UDP TPU addresses alongside QUIC`
4. `bam: support runtime client identity selection`

Agave verification results:

- `git range-diff` pairs all four commits with `=` (patch-identical).
- Aggregate stable patch ID matches:
  `3db05e567311a7d41227fb3c29b4ca71c8b691f3`.
- Changed-path sets are identical (seven files).
- All seven per-file stable patch IDs match.
- Six final blobs are identical; `core/src/banking_stage/committer.rs` differs
  only because its newer baseline differs while the BAM patch is identical.

This proves the Agave BAM stack was rebased rather than replaced or truncated.

The audit initially found a reproducibility issue outside the patch content:
the target Agave tip was not advertised by the configured upstream Agave
remote, so a clean `git submodule update` reported `not our ref
e71292b417...`. This is resolved as follows:

- Published `codex/rebase-bam-agave-v1.1-main` to
  `https://github.com/esemeniuc/agave.git` at exact tip `e71292b417...`.
- Updated `.gitmodules` to that repository and branch.
- Synchronized the local submodule URL, fetched the branch over HTTPS, and
  verified `FETCH_HEAD` and `git ls-remote` both resolve to the pinned commit.

The gitlink can now be materialized without relying on a local object cache.

### Feature inventory

| Feature surface | Port evidence | Validation evidence |
|---|---|---|
| BAM protocol, client, tile, state machine | `src/disco/bam/`, protobuf pin, gRPC transport | `test_bam_tile`, `test_bam_admin_rpc`, client/stateful fuzz targets |
| Pack/verify/dedup/resolver flow | `src/disco/{pack,verify,dedup,bam}` | `test_pack`, `test_pack_tile_bam`, `test_verify_tile`, `test_dedup_tile`, both BAM resolver tests |
| Bank/exec/poh pipeline integration | bank FFI plus `execle`, PoH, shred/topology deltas | `test_execle_tile`, `test_pohh_tile`, `test_poh_tile`, topology tests |
| Full and Frankendancer topologies | `src/app/{firedancer,fdctl}` and `src/disco/topo` | `test_fdctl_topology_bam`, `test_firedancer_topology_bam` |
| Bundle coexistence and ownership | `src/disco/bundle` and Pack wiring | `test_bundle_tile`, `test_bundle_client`, Pack tests |
| Identity, keyguard, gossip, admin | `set_identity`, keyguard/admin/gossip deltas and Agave client-identity patch | compiled in native targets; BAM admin and topology tests |
| GUI, plugin, metrics, config | GUI waterfall/plugin/metrics/config changes and generated files | `test_gui_waterfall`, `test_config_parse`, clean metrics regeneration |
| Operator docs and audit/tooling | BAM docs, dashboards, differential/fuzz tooling | path/mode inventory and syntax/static checks |
| Nested dependencies | Agave four-patch stack and BAM protobuf gitlink | patch-ID/range-diff proof and clean local checkout |

### Build and test log

The focused native GCC build with warnings-as-errors completed for these 20
test binaries:

```text
test_bam_tile test_bam_admin_rpc test_pack_tile_bam test_pack
test_dedup_tile test_bundle_tile test_bundle_client test_execle_tile
test_pohh_tile test_poh_tile test_verify_tile test_fdctl_topology_bam
test_firedancer_topology_bam test_resolv_tile_bam test_resolh_tile_bam
test_gui_waterfall test_grpc_client test_url test_config_parse
test_genesis_create
```

Seventeen passed on the first execution sweep. Three exposed current-main test
harness sizing assumptions, not production-code failures:

- `test_bundle_tile` and `test_bundle_client` allocated one MiB workspaces that
  no longer fit current transaction-v1 footprints.
- `test_resolh_tile_bam` used a 4096-byte dcache slot smaller than the current
  `FD_TPU_PARSED_MTU`, corrupting the test stack before the tile ran.

The corrective commit enlarges both bundle test workspaces and makes the BAM
resolver dcache 8192 bytes with a compile-time MTU assertion. Both
bundle tests and both resolver variants were rebuilt and passed afterward. A
subsequent clean execution sweep passed all 20 binaries in one run.

The Clang/libFuzzer build found five additional current-main harness
adaptations omitted from `ec265e2fdd`:

- Replace removed `FD_TXN_ACTUAL_SIG_MAX` with current `FD_TXN_SIG_MAX` in the
  verify-stage harness (both represent the realizable 12-signature limit on
  their respective baselines).
- Pass the new `larger_max_cost_per_block` argument to txncache footprint/new
  calls in the execle-stage harness.
- Mark the resolver startup gate started because the standalone harness has no
  replay tile/metrics topology to initialize it.
- Enlarge the BAM client fuzz workspace from one to two MiB for transaction-v1
  parsed transaction rings and the pending queue.
- Give only the stateful pipeline corpus a 3 GiB libFuzzer RSS cap. Current
  `fd_svm_mini` intentionally targets about 2 GiB before sanitizer overhead;
  measured peak RSS was 2,176,568 KiB, just above libFuzzer's 2 GiB default.

After these fixes, `fuzz_bam_client_unit` passed all 26 corpus inputs ten times
each and `fuzz_bam_pipeline_stateful_unit` passed all 16 inputs ten times each
through its ordinary make target. These five edits and the three native-test
edits are all test/fuzz-only and are included in corrective commit
`0da52d75a2` so the port preserves its validation functionality.

Additional completed checks:

- `git diff --check 45b9ece521..ec265e2fdd` and the audit worktree diff pass.
- `src/disco/metrics/gen_metrics.py` regenerated 1,763 metrics for 47 tiles
  without changing tracked output.
- All changed shell scripts pass `bash -n`; all 10 changed Python scripts parse
  to AST; all changed JSON (2), TOML (37), and YAML (5) files load successfully.
- Both new Rust utility manifests pass `cargo metadata --no-deps`.
- The native `firedancer` and combined `fdctl` executables linked and execute.
  Their `--version` output identifies audited root commit `ec265e2fdd...`.

The first combined build exhausted the `/tmp` quota during Agave compilation.
Using the existing host-disk Cargo cache, `agave-validator` built successfully
in 9m03s; the Makefile's subsequent 2.6 GiB archive copy also exceeded the same
quota, so the final link used a temporary host-disk library directory. All
temporary symlinks were removed afterward. This was an environment-capacity
failure, not a source diagnostic.

The focused Agave test initially failed to compile because baseline commit
`9d7e3c77ee` already declares `TransactionProcessingConfig` with two lifetimes
while `svm/tests/integration_test.rs` supplies one. Neither file is changed by
the four BAM commits. With a temporary diagnostic-only second lifetime added,
both `fee_only_loaded_transaction_data_size` cases (`old_fee_only` and
`simd186_fee_only`) passed; the temporary edit was then reverted and the Agave
submodule is clean at `e71292b417`.

Live BAM/Jito scenarios and differential validator runs were not executed:
their scripts require external validator/BAM services, credentials, traffic,
and multi-process operator setup not available in this local audit. Their
files, modes, configuration formats, and script syntax were verified, and the
byte/patch comparison proves their source deltas were carried.

### Readiness resolutions

1. The eight test/fuzz harness fixes are included in the follow-up corrective
   commit containing this journal.
2. Agave tip `e71292b417` is published and `.gitmodules` points to its reachable
   repository and branch.

`ec265e2fdd` alone remains the audited pre-fix snapshot.  Commit `0da52d75a2`
contains the reproducible port and validation-harness fixes; the sanitizer
hardening follow-up contains the additional production and harness corrections
documented above.

## Verification checklist

- [x] Capture exact source/target commits and baselines.
- [x] Confirm target was created from `main` and source commit was cherry-picked.
- [x] Compare complete changed-path inventories.
- [x] Prove the three source-only changes are present/superseded on target base.
- [x] Compare file status/modes and content for all 302 shared paths.
- [x] Review every post-cherry-pick adaptation for semantic preservation.
- [x] Verify nested submodule gitlinks and required submodule objects.
- [x] Inventory BAM features/tests/build registrations/config/docs/tooling.
- [x] Run whitespace and generated-metrics checks.
- [x] Run all relevant unit/integration/fuzz/static checks feasible locally.
- [x] Investigate every failure and distinguish port defects from environment or
      pre-existing failures with evidence.
- [x] Resolve the audit's harness and submodule-readiness findings.
- [x] Perform final requirement-by-requirement audit and record the result.

## Commands run

```sh
git worktree list --porcelain
git reflog show codex/rebase-5748e0c9-main
git merge-base eric/v1.1-bam codex/rebase-5748e0c9-main
git rev-list --left-right --count eric/v1.1-bam...codex/rebase-5748e0c9-main
git diff --name-only 30902441f1..6d0a4d36f8
git diff --name-only 45b9ece521..ec265e2fdd
git diff e31640e9fe..5f82da4fa0
git diff 5f82da4fa0..ec265e2fdd
git range-diff 30902441f1..6d0a4d36f8 45b9ece521..ec265e2fdd
git -C agave range-diff f69787f493..91c0cc47ac 9d7e3c77ee..e71292b417
(cd src/disco/metrics && python3 gen_metrics.py)
make -j4 test_bam_tile test_bam_admin_rpc test_pack_tile_bam test_pack \
  test_dedup_tile test_bundle_tile test_bundle_client test_execle_tile \
  test_pohh_tile test_poh_tile test_verify_tile test_fdctl_topology_bam \
  test_firedancer_topology_bam test_resolv_tile_bam test_resolh_tile_bam \
  test_gui_waterfall test_grpc_client test_url test_config_parse \
  test_genesis_create
make -j4 firedancer fdctl
build/native/gcc/bin/firedancer --version
build/native/gcc/bin/fdctl --version
make CC=clang EXTRAS=fuzz fuzz_bam_client_unit \
  fuzz_bam_pipeline_stateful_unit
cd agave && ./cargo test --profile=release-with-debug -p solana-svm \
  --test integration_test fee_only_loaded_transaction_data_size
cargo metadata --no-deps --manifest-path contrib/bam-test-server/Cargo.toml
cargo metadata --no-deps --manifest-path contrib/txnctx-bridge/Cargo.toml
git -C /tmp/firebam-agave-rebase-v1.1 push -u origin \
  codex/rebase-bam-agave-v1.1-main
git submodule sync -- agave
git -C agave fetch origin codex/rebase-bam-agave-v1.1-main
git ls-remote --heads https://github.com/esemeniuc/agave.git \
  codex/rebase-bam-agave-v1.1-main
```

## Findings and decisions

No production file, feature, or behavior is missing from the port based on the
complete path/patch audit, manual review of every divergent path, nested Agave
range-diff, successful full builds, and focused execution evidence.

The original audited commit `ec265e2fdd` was production-complete but omitted
eight current-main validation-harness adaptations and referenced an unpublished
Agave object. Both findings are fixed in the follow-up branch commit: the
focused native and fuzz suites pass, and the pinned Agave tip is fetchable from
the URL recorded in `.gitmodules`.
