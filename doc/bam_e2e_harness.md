# BAM Stateful Fuzz Harness

This doc is for the BAM model/fuzzer surface in `src/disco/bam/`.

## Files

- `test_bam_tile.c`: production BAM tile/client integration tests for protobuf ingress, result FIFO flushing, leader/control paths, and malformed-ingress rejection.
- `fuzz_bam_e2e_stateful.c`: stateful fuzz target that drives scheduler protobuf ingress through the synthetic `verify -> dedup -> resolv -> pack -> execle -> bam` path and checks the durable outbound result/leader streams.

Contract references: `BAM_docs.md`, `BAM_Validator_Spec.md`, and `src/disco/bam/`.

## Grammar

The byte grammar is stable: each event is `kind, a, b, c`; events with `kind >= 11` are ignored, and smaller values select:

| Event | Main coverage |
| --- | --- |
| `SEND_BATCH` | Synthetic valid batches delivered through production scheduler protobuf decode. |
| `REPLAY` | Same `seq_id` replay. |
| `DUP_SEQ` | Same `seq_id` with changed payload; with `c & 0x80`, partial atomic batch delivery followed by a new-`seq_id` payload switch. |
| `NEW_SEQ_SAME_PAYLOAD` | Payload replay under a new `seq_id`. |
| `ADVANCE_SLOT` | Slot rollover, stale-slot behavior, budget reset. |
| `LEADER_OFF`, `LEADER_ON` | Non-leader, missing bank, and resumed-leader paths. |
| `DISCONNECT`, `RECONNECT` | Durable result FIFO across scheduler stream resets. |
| `RESULT_BURST`, `DRAIN_QUEUE` | Result queue fill, explicit drops, and wire/model flushing. |

High-signal extensions:

- `LEADER_OFF`/`LEADER_ON` plus targeted `SEND_BATCH` events cover non-leader rejection, stale slot hints, missing leader working slot, and bank-unavailable timeout.
- `RESULT_BURST` uses bounded production-ingress batches unless `a & 0x80`; high-bit `RESULT_BURST` fills the durable result FIFO with synthetic production FIFO entries and asserts one intentional drop counter increment.
- `DRAIN_QUEUE` drains pending work and outbound results. With `c & 0x80`, the low bits are self-contained coverage checkpoints: `0x01` forces and asserts outside-slot feedback, `0x02` forces a bank transaction-error fixture, `0x04` forces result FIFO drop coverage, and `0x08` forces the alternate pack fixture transaction-error path.
- Harness shutdown drains pending transactions and the durable result queue, so seeds do not need an explicit `DRAIN_QUEUE` event to check outbound result protobuf encoding.
- Final wire/model comparison includes only results accepted into the durable BAM result FIFO. Intentionally dropped result attempts are tracked by drop counters.
- Committed wire results are compared against the model per transaction, including consumed CUs and committed transaction status.
- Generated scheduler `seq_id` values skip `UINT_MAX`, matching the BAM type contract.

The checked-in corpus should stay compact: one broad mixed trace plus targeted production-ingress rejection, valid-ingress, multi-batch, leader/bank-state, and forced-drop seeds.

## Commands

Default GCC build and smoke:

```bash
make -j fuzz_bam_e2e_stateful
build/native/gcc-12/fuzz-test/fuzz_bam_e2e_stateful corpus/fuzz_bam_e2e_stateful/*
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
