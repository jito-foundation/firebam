# BAM End-to-End Harness and Stateful Fuzz

## Scope

This harness validates BAM cleanroom contract behavior end-to-end across the Firedancer BAM ingest/scheduler boundary and a deterministic downstream pipeline model (`verify -> dedup -> resolv -> pack -> bank -> bam`).

The contract source is:

- `bam-cleanroom-spec.md` (normative behavior)
- `BAM_docs.md` (tile wiring context)

## What is covered

Deterministic matrix (`src/disco/bam/test_bam_e2e.c`):

1. Atomic all-success bundle.
2. Atomic failure at verify/sig stage.
3. Atomic failure at resolv/LUT stage.
4. Atomic runtime instruction failure in bank.
5. Partial/missing batch indices then seq switch.
6. Non-atomic single-txn success/failure.
7. Replay same batch same `seq_id`.
8. Replay same batch new `seq_id` after missing result/drop pressure.
9. Disconnect/reconnect while results are in-flight.
10. CU-limit and micro-block-limit edge/overflow.
11. Fee-accounting with mixed outcomes.
12. Forced channel saturation and explicit result drops.
13. Prevalidation: `packets.len()>5` rejection.
14. Prevalidation: inconsistent `revert_on_error` rejection.
15. Prevalidation: stale `max_schedule_slot` rejection.
16. Slot-source fallback (`leader working slot` absent -> `bankforks working slot`).
17. Non-leader phase: buffered + new work maps to `OUTSIDE_SLOT`.
18. Bank-unavailable worker path maps to `POH_TIMEOUT`.

Stateful fuzz (`src/disco/bam/fuzz_bam_e2e_stateful.c`):

- Event grammar:
  - `SEND_BATCH`
  - `REPLAY`
  - `DUP_SEQ`
  - `NEW_SEQ_SAME_PAYLOAD`
  - `ADVANCE_SLOT`
  - `LEADER_ON`
  - `LEADER_OFF`
  - `DISCONNECT`
  - `RECONNECT`
  - `FILL_QUEUE`
  - `DRAIN_QUEUE`
- Differential mode: compares SUT vs oracle fee/CU/result/drop state after every event.
- Deterministic seed from input bytes.
- Failure path writes repro artifact to `/tmp/bam_stateful_repro_<seed>.txt` including shrunk event prefix and first invariant failure snapshot.

## Contract assumptions and best-effort semantics

- BAM result delivery is best-effort under bounded queues (`try_send` semantics). Missing results are allowed and asserted via drop counters instead of exactly-once expectations.
- Replay with same or new `seq_id` must not double-charge fees or double-commit underlying work.
- Atomic batches enforce rollback semantics (`COMMIT_CANCELLED` for non-primary peers).
- Slot-source rule uses leader working slot when present, otherwise bankforks working slot.
- Auth flow assertions include `AUTH_LABEL`-prefixed signing payload and on-wire `AuthProof` field validation.
- In this standalone unit harness, drop-pressure assertions are validated at the BAM result queue boundary (pending/head/tail/drop counters) rather than through `fd_bam_send_result` metric side effects.

## Run commands

From repo root:

```bash
make -j4 build/native/gcc/unit-test/test_bam_e2e
build/native/gcc/unit-test/test_bam_e2e
make -j4 build/native/gcc/fuzz-test/fuzz_bam_e2e_stateful
```

Stateful fuzz smoke (stub engine build):

```bash
printf '\x01\x02\x03\x04' >/tmp/bam_fuzz_seed.bin
build/native/gcc/fuzz-test/fuzz_bam_e2e_stateful /tmp/bam_fuzz_seed.bin
```

LibFuzzer mode (if built with `EXTRAS=fuzz`) can use a time budget:

```bash
build/native/clang/fuzz-test/fuzz_bam_e2e_stateful -max_total_time=60 <corpus_dir>
```

On invariant failure, rerun with the generated repro artifact (`/tmp/bam_stateful_repro_<seed>.txt`).
