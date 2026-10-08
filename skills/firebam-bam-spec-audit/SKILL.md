---
name: firebam-bam-spec-audit
description: Audit FireBAM against bam_spec.md and the BAM reference validator. Use for protocol conformance reviews or discrepancies in BAM wire, scheduling, execution, or result semantics.
---

# FireBAM BAM Spec Audit

## Rules

Use `bam_spec.md` as the clean-room wire-contract baseline for the BAM node and
reference Jito-Solana validator revisions recorded in that file. For this
FireBAM branch, live code under `src/` is authoritative. Recheck behavior and
limits that may have changed after the recorded revisions against the live BAM,
FireBAM, and available sibling source trees.

When judging behavior, label it precisely:

- `spec conformant`: required by `bam_spec.md` and implemented or tested at the right layer.
- `implementation-specific`: matches current code but is stricter, looser, or different from `bam_spec.md`.
- `generic regression`: useful Firedancer coverage, but not BAM protocol evidence.
- `missing coverage`: spec behavior is not directly exercised where it is implemented.

Call out disagreements between code, tests, comments, and `bam_spec.md` explicitly.

## Trace Targets

For BAM execution, scheduling, or feedback issues, start near these files and use `rg` to find the active path:

- `src/disco/fd_txn_m.h`
- `src/disco/bam/fd_bam_client.c`
- `src/disco/bam/fd_bam_client_decode.c`
- `src/disco/bam/fd_bam_tile.c`
- `src/disco/pack/fd_pack.c`
- `src/disco/pack/fd_pack_tile.c`
- `src/discoh/bank/fd_bank_tile.c`
- `src/disco/verify/fd_verify_tile.c`

Spec areas that have produced subtle bugs in this branch:

- `AtomicTxnBatch` validation: non-empty batch, maximum batch size, consistent `revert_on_error`, slot window, signature verification, parsing, and sanitize failures.
- Scheduling: leader ownership, `max_schedule_slot`, omitted/zero slot-hint handling, non-leader flushes, stale work eviction, and `seq_id` conflict ordering.
- Execution: `revert_on_error`, lock or execute failures, `POH_TIMEOUT`, bank availability, and committed vs not-committed outcomes.
- Result construction: exactly one result per handled batch, reason precedence, transaction index mapping, sanitize markers, execution success, CUs, fees, loaded account data size, and feepayer balances.
- Delivery: durable FIFO result links vs latest-value-wins leader snapshots.

## High-Signal Distinctions

For interpreting BAM ingress/slot-timing logs or comparing late ingress with
pack outcomes, read [timing-log interpretation](references/timing-logs.md).

### BAM Resends And Prepack Dedup

- Treat same-signature BAM traffic as a possible intentional resend, not generic HA duplicate traffic.
- Early signature dedup is controlled by `fd_txn_m_use_prepack_sig_dedup(...)` in `src/disco/bam/fd_bam_txn_m.h` (verify); dedup handles BAM in `src/disco/dedup/fd_dedup_tile_bam.c`.
- Current contract: early signature dedup is disabled for block-engine bundles and BAM traffic, and remains enabled for normal QUIC/UDP traffic.
- If verify or dedup drops a resent BAM signature before pack sees it, treat that as a likely regression unless the drop is explicitly on an executed-signature or other terminal path.
- Pack is the BAM-aware stage that should classify resend work against `seq_id`, `max_schedule_slot`, leader state, and downstream execution state.

### No-Leader Gap Behavior

- `ctx->leader_slot==ULONG_MAX` in pack means "no active local leader slot right now".
- In that state, pack does not publish a normal leader snapshot, so BAM log state can trail pack's true local state.
- The `bam_min_admission_slot` floor still rejects closed-slot work during a no-leader gap; finishing a leader slot advances that floor beyond the finished slot.
- Ingress lateness alone does not prove that a batch's slot limit is expired or require an immediate `rejected_pre_pending_outside_slot` log.

### `max_schedule_slot` Semantics

- `max_schedule_slot` is a scheduler-provided slot limit, not a generic "current slot" field.
- Protobuf omission of `max_schedule_slot` decodes as `0`. Current FireBAM carries that value through as the BAM slot/result slot and applies the ordinary slot-window checks; once a current execution slot is known, `0` is normally stale. There is no special missing-slot-hint metric in this tree.
- For a zero-slot case, trace decode, pack validation, and result construction to distinguish omitted protobuf data from an explicitly encoded zero.
- When auditing stale-slot rejections, compare pack's computed minimum acceptable slot against BAM's `max_schedule_slot`, not against BAM's log timestamp alone.

### Pack-Side Stale-Slot Reasoning

- For pack-side stale-slot debugging, the most important computed field is `required_min_slot`.
- With a known leader slot, `required_min_slot` is `max(leader_slot, bam_min_admission_slot)`; in a no-leader gap, a nonzero admission floor can still establish that minimum.
- `blockhash_height` (the resolver's reference block height: the blockhash's block height, or the resolver's current block height when the blockhash is unknown; minimum across members for a bundle; `blockhash_height_known=0` if no member arrived; a block height, not a slot) and `highest_observed_block_height` are logged separately and do not define the BAM slot-admission minimum.
- `rejected_pre_pending_outside_slot` means `max_schedule_slot` is the invalid `ULONG_MAX` sentinel or falls below the admission floor or known leader slot. Trace these checks in `src/disco/pack/fd_pack_tile.c`.

## Test Signal

Classify tests by the layer they exercise:

- `src/disco/bam/test_pack_bam.c`: `fd_pack.c` scheduling semantics. BAM `seq_id` ordering tests are spec-relevant when they exercise BAM conflict order or bypass behavior. Generic bundle or initializer-pack tests are pack regressions, not BAM spec proof.
- `src/disco/bam/test_pack_tile_bam.c`: direct `fd_pack_tile.c` BAM helper and result-mapping coverage. Use this for stale `max_schedule_slot`, insert rejection mapping, tracking rejection mapping, and pack-to-BAM result publication paths.
- `src/disco/bam/test_bam_tile.c`: BAM tile integration, decode, ingress, and feedback-link contracts.
- `src/disco/verify/test_verify_tile.c`: verify behavior for BAM parse/signature failures and prepack signature-dedup policy.
- `src/disco/bam/test_dedup_tile.c`: dedup behavior across BAM, bundle, and ordinary TPU sources.
- `src/disco/bam/fuzz_bam_pipeline_stateful.c`: stateful real-path pipeline coverage through synthetic links. It uses a shadow result FIFO, not a separate scheduler model oracle.

For resends or prepack dedup, use the BAM cases in
`src/disco/verify/test_verify_tile.c`, `src/disco/bam/test_dedup_tile.c`, and
`src/disco/bam/test_pack_tile_bam.c`. BAM duplicates should survive the early
verify/dedup policy, while pack classifies same-signature work using its BAM
tracking state.

Flag low-signal coverage when a test only validates a synthetic model, duplicates stronger pipeline coverage, or asserts generic Firedancer behavior unrelated to BAM semantics.
