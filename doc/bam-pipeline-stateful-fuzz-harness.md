# BAM Pipeline Stateful Fuzz Harness

This doc covers the stateful BAM pipeline fuzzer in
[`src/disco/bam/`](../src/disco/bam/).

## Files

- `test_bam_tile.c`: production BAM tile/client integration tests for protobuf ingress, result FIFO flushing, leader/control paths, and malformed-ingress rejection.
- `fuzz_bam_pipeline_stateful.c`: stateful fuzz target that drives scheduler protobuf ingress through production callbacks connected by synthetic `verify -> dedup -> resolv -> pack -> execle` links, feeds pack/execle failure results back to BAM, and checks outbound result/leader protobuf encoding.
- `fuzz_bam_pipeline_stage_*.{c,h}`: per-stage adapters used by the stateful target; these are not standalone fuzz targets.
- `fuzz_bam_pipeline_links.h`: shared synthetic mcache/dcache link helpers for the stage adapters.

Contract references: [bam_spec.md](../bam_spec.md), [AGENTS.md](../AGENTS.md),
and the live [harness](../src/disco/bam/fuzz_bam_pipeline_stateful.c) and stage
adapters. Live code is authoritative.

## Coverage boundary

The adapters call real verify, dedup, resolver, pack, and native Firedancer
execle/runtime code with signature-valid fixtures, seeded account/blockhash
state, and miniature banks. Links and tile clocks are synthetic; pack's BAM
ownership is fixed active and output credits are effectively unlimited. This
target does not exercise concurrent stem operation, real link backpressure,
ownership handoffs, transport authentication/TLS, or Frankendancer bank/pohh
execution.

There is no PoH/motor adapter. The harness counts `execle_poh` outputs but
does not turn their provisional committed outcomes into `poh_bam` feedback.
Execution feedback consumed here comes from `bank_bam` not-committed outcomes;
synthetic successful result entries exercise committed-result wire encoding.
The shadow FIFO checks queue/encoding consistency, not an independent model
of scheduling, execution correctness, or CU accounting.

## Grammar

Each event is four bytes: `kind, a, b, c`. The harness processes at most
`BAM_FUZZ_MAX_EVENTS` (currently 128) records. Incomplete trailing bytes and
records beyond that limit are ignored. Invalid kinds (`kind >= 11`) are ignored
but still consume a record within the limit.

| Kind | Event | Main coverage |
| --- | --- | --- |
| `0` | `SEND_BATCH` | Signature-valid fixture batches through production scheduler protobuf decode. |
| `1` | `REPLAY` | Replay the remembered batches with the same `seq_id`; create remembered batches first if absent. |
| `2` | `DUP_SEQ` | Replay with changed fixture selectors (which may still select identical bytes). With `c & 0x80`, submit a 2–5-transaction atomic batch, perform one BAM publication callback, then submit a new-`seq_id` fixture selection; publication remains whole-batch. |
| `3` | `NEW_SEQ_SAME_PAYLOAD` | Replay remembered payloads under new sequence IDs. |
| `4` | `ADVANCE_SLOT` | Slot rollover, stale-slot behavior, and budget reset. |
| `5`, `6` | `LEADER_OFF`, `LEADER_ON` | Non-leader/null execution-bank state and resumed-leader state with a valid bank. |
| `7`, `8` | `DISCONNECT`, `RECONNECT` | Queued result retention across synthetic scheduler stream resets. |
| `9`, `10` | `RESULT_BURST`, `DRAIN_QUEUE` | Result queue fill, intentional drops, and wire/shadow-FIFO comparison. |

High-signal extensions:

- `LEADER_OFF`/`LEADER_ON` plus targeted `SEND_BATCH` events exercise non-leader pending/scheduling behavior and stale-slot handling. `LEADER_ON` prepares a valid execution bank; the target does not independently model a leader with an unavailable bank.
- `RESULT_BURST` uses bounded production-ingress batches unless `a & 0x80`; high-bit `RESULT_BURST` fills the durable result FIFO with synthetic production FIFO entries and asserts one intentional drop counter increment.
- `DRAIN_QUEUE` pumps pending work and attempts outbound result flushing. With `c & 0x80`, the low bits are self-contained coverage checkpoints: `0x01` forces and asserts outside-slot feedback, `0x02` forces a native execle transaction-error fixture, `0x04` forces result FIFO drop coverage, and `0x08` forces the alternate pack fixture transaction-error path.
- Transaction-error checkpoints start a fresh leader slot beyond the preceding input's possible pending targets and require a transaction-error result for one of their own sequence IDs.
- Harness shutdown drains BAM's pending publication queue, performs a final pack pump of up to eight iterations, and attempts outbound FIFO flushing. Seeds do not need an explicit `DRAIN_QUEUE` to invoke wire checks, but shutdown does not assert that every pack transaction completed or the result FIFO became empty; flushing can stop while the TX path is busy.
- Final wire/FIFO comparison includes only results accepted into the durable BAM result queue. Intentionally dropped result attempts are tracked by drop counters.
- Committed wire results are compared with synthetic successful shadow-FIFO entries per transaction, including consumed CUs and committed transaction status. This does not verify PoH acceptance of native execle output.
- Generated scheduler `seq_id` values use wrapping `uint` arithmetic; every `uint` value, including `UINT_MAX`, is valid under the BAM type contract.

The checked-in corpus should stay compact: one broad mixed trace plus targeted production-ingress rejection, valid-ingress, multi-batch, leader/bank-state, and forced-drop seeds.

## Commands

Default GCC build and smoke:

```bash
make -j fuzz_bam_pipeline_stateful
"$(make --silent objdir)/fuzz-test/fuzz_bam_pipeline_stateful" corpus/fuzz_bam_pipeline_stateful
```

LibFuzzer:

```bash
make CC=clang CXX=clang++ EXTRAS="fuzz asan" fuzz_bam_pipeline_stateful
"$(make --silent CC=clang CXX=clang++ EXTRAS="fuzz asan" objdir)/fuzz-test/fuzz_bam_pipeline_stateful" corpus/fuzz_bam_pipeline_stateful
```

The GCC target uses the corpus replay stub; it performs no fuzz exploration.
Use the same build parameters when querying `objdir`. For an explicit separate
output tree, use a flat name such as `BUILDDIR=clang-fuzz-asan` on both commands.

AFL++:

```bash
make CC=clang CXX=clang++ EXTRAS=afl++ AFL_LIB=/usr/lib/afl fuzz_bam_pipeline_stateful
AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1 AFL_SKIP_CPUFREQ=1 afl-fuzz -i corpus/fuzz_bam_pipeline_stateful -o findings -- "$(make --silent CC=clang CXX=clang++ EXTRAS=afl++ AFL_LIB=/usr/lib/afl objdir)/fuzz-test/fuzz_bam_pipeline_stateful"
```

Set `AFL_LIB` to the installation containing `afl-compiler-rt.o` and
`libAFLDriver.a`. The harness reserves an 8 GiB demand-paged anonymous workspace;
it does not require preallocated huge pages. Runtime, sanitizer, and corpus
state still consume resident memory.

On failure, pass the libFuzzer/AFL artifact or failing corpus file as an
argument to the matching instrumented binary to reproduce it.
