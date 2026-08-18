# Port log: FireBAM `main` commits to `v26.08`

Date: 2026-08-28

## Scope

- Target branch before the port: `v26.08` at `dcf3a22a19154400409f71a9731748b411f14d93` (`v26.08.2`).
- Source BAM commit: `18517726ae23553d47c37efcd8c580c0c823fd36` (`bam: port BAM patch to main (#29)`).
- Source documentation follow-up: `2cd0e823217caa53748bcdd8fcad9c00ab06d148` (`docs: consolidate FireBAM audit guidance`).
- The port initially used `a4dfe50cf368941675dd86d9c5ee3388e7c98a4c` before `origin/main` was force-updated; the exact delta to its replacement was subsequently applied.
- The existing untracked audit, handoff, backup, patch, and checked-out `bam-protos` files were preserved. The existing `bam-protos` checkout was already at the required `9d8f13df8db085099bfd7ebe3a53a4ea0df76762` revision.

## Port procedure

1. Inspected the branch, the source commit, the common ancestor, submodule revisions, and untracked-file overlap.
2. Started a cherry-pick of `a4dfe50cf3`. The repository's optional commit-graph cache reported `invalid commit position`, so subsequent cherry-pick operations used `-c core.commitGraph=false`; no repository metadata was rewritten.
3. Resolved the superproject conflicts while preserving the `v26.08` APIs and topology conventions.
4. Regenerated all metric sources and `book/api/metrics-generated.md` from the merged `src/disco/metrics/metrics.xml` with `make metrics`.
5. Replayed the four Agave commits represented by the source gitlink change onto the existing `v26.08` Agave base rather than replacing it with the source branch's unrelated Agave history.
6. Added the small source-parent prerequisites exposed by focused compilation, then rebuilt and ran the affected tests.

## Conflict resolutions

- Preserved the release's `fd_obj_cb_backup_vis` naming, current PoH join/mixin signatures, wallclock projection, deleted `fd_config_json.c`, and single shared `pack_execle` link.
- Added only the BAM object callbacks; the source branch's removed leader-transaction-timing object was not reintroduced.
- Integrated BAM topology, control, signing, feedback, shred, gossip, GUI, pack, bank/execle, PoH, replay, metrics, configuration, and test paths into both Firedancer and Frankendancer.
- Kept the release's pack drain behavior and moved it ahead of the non-leader early return so completed slots can drain correctly.
- Adapted BAM microblock parsing to the release's clock and PoH APIs.
- Kept the release's current URL parser behavior; a source-parent-only query/fragment URL test was not imported.
- Regenerated metric artifacts rather than selecting either generated side of the conflict.

## Compatibility prerequisites added

The source commit assumed these facilities already existed on its parent, while `v26.08` did not contain them:

- Shared URL/FQDN/SNI storage bounds used by BAM topology and control structures.
- `first_seen_nanos` propagation from BAM ingress through `fd_txn_m_t` and `fd_txn_p_t` into pack telemetry.
- Bundle-crank signer/program authority fields and generated transaction masks used by the BAM-only crank authorization path.
- HTTP/2 stream-state validation before sending additional gRPC stream messages.

## Agave submodule

- Existing release base: `9d7e3c77ee3ad84cbebc246e0e4f5d55cca6129a`.
- Source gitlink delta: `e9c2389bf86c71dd58a91a9e5d30cd2e7baa4fa2..fafa43c45f29bb12aa6be0a9a09ed4ee3feeadf3`.
- Replayed commits:
  - `ac44c659aa` — report fee-payer balance and loaded account data size
  - `cab6eff26d` — expose transaction batch failure policy through bank FFI
  - `258851e9c6` — publish UDP TPU addresses alongside QUIC
  - `fafa43c45f` — support runtime client identity selection
- Ported Agave tip recorded by the superproject: `f66f6bf7db6aaf07dd40f8d7ee7899879e916d5d`.
- The bank FFI conflict was adapted to this release's `Executed`/`FeesOnly` transaction result variants; the newer source-only `NoOp` variant was not introduced.

## Validation

Focused build completed successfully:

```text
make -j4 test_config_parse test_url test_bundle_crank test_bam_tile \
  test_pack_tile_bam test_poh_tile test_pohh_tile test_replay_tile \
  test_firedancer_topology_bam test_fdctl_topology_bam
```

Passing test binaries:

- `test_config_parse`
- `test_url`
- `test_bundle_crank`
- `test_bam_tile`
- `test_pack_tile_bam`
- `test_poh_tile`
- `test_pohh_tile`
- `test_replay_tile --page-sz normal --page-cnt 262144`
- `test_firedancer_topology_bam`
- `test_fdctl_topology_bam`

Agave checks completed successfully:

```text
cargo check -p solana-core
cargo check -p agave-validator --lib
```

Validation caveats:

- `cargo fmt --all -- --check` is not clean for this Agave fork because the available stable formatter rejects its nightly-only settings and reports broad pre-existing formatting drift outside this port.
- Checking all `agave-validator` binary targets reaches unrelated fork wiring errors in `validator/src/main.rs` and `validator/src/bin/solana-test-validator.rs`; the affected validator library and `solana-core` targets pass.

## Follow-up synchronization with rewritten `origin/main`

Later on 2026-08-28, `origin/main` was force-updated so the original source commit
`a4dfe50cf3` was replaced by `18517726ae`, followed by documentation commit
`2cd0e82321`.

- `a4dfe50cf3` and `18517726ae` have the same parent, `6eca50cd36`.
- The complete tree delta between the two source commits contains only two changes:
  removal of `ANTHONY_SLACK_FINDINGS_AUDIT.md` and expansion of
  `test_fdctl_topology_bam` to verify the BAM-to-plugin link when both BAM and the
  GUI are enabled.
- That exact incremental delta is included in the final squashed port. Before
  squashing, its stable patch ID,
  `fe71487b0cfabf6936d97bdec9451ce7aab3c156`, matched the source-tree delta
  `a4dfe50cf3..18517726ae`.
- `2cd0e82321` was applied without conflicts and is included in the final squashed
  port. Before squashing, its stable patch ID matched the source commit's patch ID,
  `6896c3917c5817883b5df4f8ca33c454444ebcb4`.
- The documentation consolidation updates `AGENTS.md`, `bam_spec.md`, and the
  upstream-rebase audit skill, and removes the superseded rebase plan and local
  Prometheus helper files.

Focused follow-up validation passed:

```text
make -j4 test_fdctl_topology_bam
build/native/gcc/unit-test/test_fdctl_topology_bam
```
