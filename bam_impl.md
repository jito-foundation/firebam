You are implementing BAM behavior in `/home/eric/dev/firebam`.

    Goal:
    Apply `bam-cleanroom-spec.md` to Firebam, with primary focus on this property:
    1. Bundles must execute in `seq_id` order when they are contentious (conflicting account access).
    2. Non-contentious bundles are allowed to execute out of strict `seq_id` order.

    Source of truth:
    1. `bam-cleanroom-spec.md` (especially sections on `seq_id` priority, prio-graph conflict semantics, insertion order,
  revert_on_error atomicity, and execution/recording order).
    2. `BAM_docs.md` + AGENTS tile flow expectations.
    3. Existing BAM code paths in:
    `src/disco/bam/fd_bam_client_decode.c`
    `src/disco/bam/fd_bam_client.c`
    `src/disco/pack/fd_pack_tile.c`
    `src/disco/pack/fd_pack.c`
    `src/discof/bank/fd_bank_tile.c`
    `src/disco/bam/test_bam_e2e.c`
    `src/disco/bam/test_bam_tile.c`

    Implementation requirements:
    1. Preserve `AtomicTxnBatch` semantics: `seq_id`, `max_schedule_slot`, `revert_on_error`, one result per batch.
    2. Keep intra-batch transaction order fixed as received.
    3. Enforce contention-aware ordering:
       - If two BAM bundles conflict on account access (R/W rules from cleanroom spec), lower `seq_id` must win.
       - If two BAM bundles do not conflict, allow scheduling/execution without strict FIFO blocking on older `seq_id`.
    4. Keep `revert_on_error=true` atomic behavior unchanged.
    5. Preserve current prevalidation and decode constraints (mixed revert flag rejection, batch size limits, non-revert multi-
  packet behavior if still intended).
    6. Do not regress BAM connectivity/auth/config/gossip updates.

    Testing requirements:
    1. Add/extend tests to prove:
       - Conflicting bundles execute in `seq_id` order.
       - Non-conflicting bundles can bypass/reorder relative to strict FIFO.
       - Intra-bundle order is preserved.
       - Results are still correlated correctly by `seq_id`.
    2. Update existing BAM e2e/unit tests where behavior changes are intentional.
    3. Run relevant BAM/pack/bank tests and report exact commands + pass/fail summary.

    Output format:
    1. Brief summary of behavior changes.
    2. File-by-file diff rationale.
    3. Test evidence.
    4. Any remaining gaps or follow-up items.

