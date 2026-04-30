---
name: firebam-bam-spec-audit
description: Audit, fix, or review FireBAM behavior against bam_spec.md. Use when working in this repo on BAM protocol conformance, BAM tests, result mapping, max_schedule_slot handling, seq_id ordering, revert_on_error semantics, pack/bank/bam tile feedback, or comparing Firedancer BAM code with the reference BAM client.
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

- `src/disco/bam/fd_bam_client.c`
- `src/disco/bam/fd_bam_client_decode.c`
- `src/disco/bam/fd_bam_tile.c`
- `src/disco/pack/fd_pack.c`
- `src/disco/pack/fd_pack_tile.c`
- `src/discof/bank/fd_bank_tile.c`
- `src/discoh/bank/fd_bank_tile.c`
- `src/disco/verify/fd_verify_tile.c`

Spec areas that have produced subtle bugs in this branch:

- `AtomicTxnBatch` validation: non-empty batch, maximum batch size, consistent `revert_on_error`, slot window, signature verification, parsing, and sanitize failures.
- Scheduling: leader ownership, `max_schedule_slot`, non-leader flushes, stale work eviction, and `seq_id` conflict ordering.
- Execution: `revert_on_error`, lock or execute failures, `POH_TIMEOUT`, bank availability, and committed vs not-committed outcomes.
- Result construction: exactly one result per handled batch, reason precedence, transaction index mapping, sanitize markers, execution success, CUs, fees, loaded account data size, and feepayer balances.
- Delivery: durable FIFO result links vs latest-value-wins leader snapshots.

## Test Signal

Classify tests by the layer they exercise:

- `src/disco/pack/test_pack.c`: `fd_pack.c` scheduling semantics. BAM `seq_id` ordering tests are spec-relevant when they exercise BAM conflict order or bypass behavior. Generic bundle or initializer-pack tests are pack regressions, not BAM spec proof.
- `src/disco/pack/test_pack_tile_bam.c`: direct `fd_pack_tile.c` BAM helper and result-mapping coverage. Use this for stale `max_schedule_slot`, insert rejection mapping, tracking rejection mapping, and pack-to-BAM result publication paths.
- `src/disco/bam/test_bam_tile.c`: BAM tile integration, decode, ingress, and feedback-link contracts.
- `src/disco/bam/test_bam_model.c`: model coverage. Treat as lower signal for implementation conformance if the test simulates scheduling or execution instead of driving real pack, bank, or worker code.
- `src/disco/verify/test_bam_prepack_pipeline.c`: verify and dedup pipeline behavior for BAM versus QUIC or bundle paths.

Flag low-signal coverage when a test only validates a synthetic model, duplicates stronger pipeline coverage, or asserts generic Firedancer behavior unrelated to BAM semantics.
