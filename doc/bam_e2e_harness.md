# BAM Stateful Fuzz Harness

This doc is for the BAM model/fuzzer surface in `src/disco/bam/`.

## Files

- `test_bam_tile.c`: production BAM tile/client integration tests for protobuf ingress, result FIFO flushing, leader/control paths, and malformed-ingress rejection.
- `test_bam_model.c`: deterministic model for the scheduler stream through the synthetic `verify -> dedup -> resolv -> pack -> bank -> bam` path.
- `fuzz_bam_e2e_stateful.c`: stateful fuzz target that reuses the model and also drives production scheduler-ingress decode for raw BAM batches.

Contract references: `BAM_docs.md`, `BAM_Validator_Spec.md`, and `src/disco/bam/`.

## Grammar

The byte grammar is stable: each event is `kind, a, b, c`, with `kind % 11` selecting:

| Event | Main coverage |
| --- | --- |
| `SEND_BATCH` | Synthetic valid batches; with `c & 0x80`, production scheduler-ingress decode. |
| `REPLAY` | Same `seq_id` replay. |
| `DUP_SEQ` | Same `seq_id` with changed payload; with `c & 0x80`, partial atomic batch then sequence switch. |
| `NEW_SEQ_SAME_PAYLOAD` | Payload replay under a new `seq_id`. |
| `ADVANCE_SLOT` | Slot rollover, stale-slot behavior, budget reset. |
| `LEADER_OFF`, `LEADER_ON` | Non-leader, missing bank, and resumed-leader paths. |
| `DISCONNECT`, `RECONNECT` | Durable result FIFO across scheduler stream resets. |
| `FILL_QUEUE`, `DRAIN_QUEUE` | Result queue fill, explicit drops, and wire/model flushing. |

High-signal extensions:

- `SEND_BATCH` uses the original synthetic valid-batch path unless `c & 0x80`.
- High-bit `SEND_BATCH` drives production scheduler-ingress decode.
- With `b & 0x40` clear, high-bit `SEND_BATCH` covers: empty batch, too many packets, non-atomic multi-packet batch, mixed `revert_on_error`, oversize metadata, simple vote transaction, and with `b & 0x80`, too many `AtomicTxnBatch` entries in one wrapper.
- With `b & 0x40` set, high-bit `SEND_BATCH` sends valid Solana transaction fixtures as either one transaction or an atomic bundle; with `b & 0x80`, it sends multiple valid batches in one scheduler response.
- `LEADER_OFF`/`LEADER_ON` plus targeted `SEND_BATCH` events cover non-leader rejection, stale slot hints, missing leader working slot, and bank-unavailable timeout.
- `FILL_QUEUE` uses bounded direct-result fill unless `a & 0x80`; high-bit `FILL_QUEUE` saturates the result FIFO and asserts one intentional drop.
- Final wire/model comparison includes only results accepted into the durable BAM result FIFO. Intentionally dropped result attempts are tracked by drop counters.
- Committed wire results are compared against the model per transaction, including consumed CUs and committed transaction status.

The checked-in corpus should stay compact: one broad mixed trace plus targeted production-ingress rejection, valid-ingress, multi-batch, leader/bank-state, and forced-drop seeds.

## Commands

Default GCC build and smoke:

```bash
make -j test_bam_model fuzz_bam_e2e_stateful
build/native/gcc-12/unit-test/test_bam_model
build/native/gcc-12/fuzz-test/fuzz_bam_e2e_stateful corpus/fuzz_bam_e2e_stateful/*
make fuzz_bam_e2e_stateful_unit
```

LibFuzzer:

```bash
make CC=clang CXX=clang++ EXTRAS="fuzz asan" fuzz_bam_e2e_stateful
build/native/clang/fuzz-test/fuzz_bam_e2e_stateful corpus/fuzz_bam_e2e_stateful
```

Use `BUILDDIR=native/clang-fuzz` only when you want a separate clang fuzz output tree.

AFL++:

```bash
make CC=clang CXX=clang++ EXTRAS=afl++ AFL_LIB=/usr/lib/afl fuzz_bam_e2e_stateful
AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1 AFL_SKIP_CPUFREQ=1 afl-fuzz -i corpus/fuzz_bam_e2e_stateful -o findings -- build/native/clang/fuzz-test/fuzz_bam_e2e_stateful
```

On failure, reproduce with the libFuzzer artifact or the failing corpus path printed by the runner.
