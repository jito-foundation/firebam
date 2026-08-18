---
name: firebam-bam-spec-audit
description: Audit, fix, review, or explain FireBAM behavior against bam_spec.md. Use when working in this repo on BAM protocol conformance, BAM resend semantics, prepack verify/dedup behavior, BAM logs, result mapping, max_schedule_slot handling, seq_id ordering, revert_on_error semantics, pack/bank/bam tile feedback, or comparing Firedancer BAM code with the reference BAM client.
---

# FireBAM BAM Spec Audit

## Rules

Use `bam_spec.md` as the normative source for BAM behavior. Treat `BAM_SPEC_PROMPT.md` as separate prompt material and leave it untouched unless explicitly asked.

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

### BAM Resends And Prepack Dedup

- Treat same-signature BAM traffic as a possible intentional resend, not generic HA duplicate traffic.
- Early signature dedup is controlled by `fd_txn_m_use_prepack_sig_dedup(...)` in `src/disco/fd_txn_m.h`.
- Current contract: early signature dedup is disabled for block-engine bundles and BAM traffic, and remains enabled for normal QUIC/UDP traffic.
- If verify or dedup drops a resent BAM signature before pack sees it, treat that as a likely regression unless the drop is explicitly on an executed-signature or other terminal path.
- Pack is the BAM-aware stage that should classify resend work against `seq_id`, `max_schedule_slot`, leader state, and downstream execution state.

### Ingress Timing Versus Pack Outcomes

- `BAM rx bundle: ...` and `firedancer_slot_timing: ...` are BAM-ingress observations emitted by `src/disco/bam/fd_bam_client_decode.c`.
- `BAM ingress vs Firedancer slot summary: ...` is a BAM rollup emitted by `src/disco/bam/fd_bam_tile.c`.
- `bam_drop ...` is a pack-side outcome emitted by `src/disco/pack/fd_pack_tile.c`.
- Do not infer `bam_drop` 1:1 from `txns_after_slot_end>0`. "Late at BAM ingress" and "rejected by pack" are related but distinct facts.

### `current_leader_slot` Provenance

- In BAM logs, `current_leader_slot` comes from `ctx->bam_leader_state.slot`, which is sourced from pack leader snapshots over `pack_bam_ldr`.
- It does not come from the BAM batch itself.
- If pack is in a no-leader gap, BAM can still log an older `current_leader_slot` until a newer pack leader snapshot arrives.

### No-Leader Gap Behavior

- `ctx->leader_slot==ULONG_MAX` in pack means "no active local leader slot right now".
- In that state, pack does not publish a normal leader snapshot, so BAM log state can trail pack's true local state.
- `outside_slot` behavior also changes in this gap: if pack does not know a current slot, the rejection path is narrower than the usual `max(current_slot, blockhash_slot)` check.
- Therefore, a BAM txn can be late at ingress and still not produce an immediate `rejected_pre_pending_outside_slot` log.

### `max_schedule_slot` Semantics

- `max_schedule_slot` is a scheduler-provided slot limit, not a generic "current slot" field.
- Protobuf omission of `max_schedule_slot` decodes as `0`. Current Firedancer carries that `0` through as the BAM slot hint/result slot and comments treat it as "no slot hint"; there is no `bam_ingress_batch_no_max_schedule_slot_provided` metric in this tree.
- Before calling a zero-slot case a stale-slot bug, trace decode, pack validation, and result construction for the active path.
- When auditing stale-slot rejections, compare pack's computed minimum acceptable slot against BAM's `max_schedule_slot`, not against BAM's log timestamp alone.

### Reading Slot-Timing Logs

- `first_rx_minus_slot_end_ns > 0` means the first observed txn for that slot arrived after slot end when slot-end timing is known.
- `txns_before_slot_end`, `txns_after_slot_end`, and `txns_unknown_slot_end` are per-slot counters, not per-batch counters.
- If slot-end timing is unknown, BAM falls back to the coarser slot-number test when deciding whether a txn is late.
- A `current_leader_slot` equal to `max_schedule_slot` does not prove the txn was on time; same-slot late arrivals are possible when the receive timestamp is already past slot end.

### Pack-Side Stale-Slot Reasoning

- For pack-side stale-slot debugging, the most important computed field is `required_min_slot`.
- When pack knows a validation slot, `required_min_slot` is effectively `max(validation_slot, blockhash_slot)`.
- A `rejected_pre_pending_outside_slot` outcome means BAM's `max_schedule_slot` trailed that requirement.

## Test Signal

Classify tests by the layer they exercise:

- `src/disco/pack/test_pack.c`: `fd_pack.c` scheduling semantics. BAM `seq_id` ordering tests are spec-relevant when they exercise BAM conflict order or bypass behavior. Generic bundle or initializer-pack tests are pack regressions, not BAM spec proof.
- `src/disco/pack/test_pack_tile_bam.c`: direct `fd_pack_tile.c` BAM helper and result-mapping coverage. Use this for stale `max_schedule_slot`, insert rejection mapping, tracking rejection mapping, and pack-to-BAM result publication paths.
- `src/disco/bam/test_bam_tile.c`: BAM tile integration, decode, ingress, and feedback-link contracts.
- `src/disco/bam/test_bam_model.c`: model coverage. Treat as lower signal for implementation conformance if the test simulates scheduling or execution instead of driving real pack, bank, or worker code.
- `src/disco/verify/test_bam_prepack_pipeline.c`: verify and dedup pipeline behavior for BAM versus QUIC or bundle paths.

Use these specific tests when the issue involves resends or prepack dedup:

- `src/disco/verify/test_bam_prepack_pipeline.c`: BAM duplicates should survive verify+dedup prepack handling, while QUIC duplicates should still dedup.
- `src/disco/bam/test_bam_model.c`: replay scenarios are useful as contract signal for same-signature/same-or-new-`seq_id` behavior, but are still lower-signal than real pipeline coverage.

Flag low-signal coverage when a test only validates a synthetic model, duplicates stronger pipeline coverage, or asserts generic Firedancer behavior unrelated to BAM semantics.
