# BAM timing-log interpretation

Use this for BAM ingress/slot-timing log analysis. Paths below are relative to
the FireBAM repository root. Keep the audit entrypoint's
[slot-window rules](../SKILL.md#max_schedule_slot-semantics) in view when
comparing observations with scheduling outcomes.

## Ingress timing versus pack outcomes

- `BAM rx bundle: ...` and `firedancer_slot_timing: ...` are BAM-ingress observations emitted by `src/disco/bam/fd_bam_client_decode.c`.
- `BAM ingress vs Firedancer slot summary: ...` is a BAM rollup emitted by `src/disco/bam/fd_bam_tile.c`.
- `bam_drop ...` is a pack-side outcome emitted by `src/disco/pack/fd_pack_tile.c`.
- Do not infer `bam_drop` 1:1 from `txns_after_slot_end>0`. "Late at BAM ingress" and "rejected by pack" are related but distinct facts.

## `current_leader_slot` provenance

- In BAM logs, `current_leader_slot` comes from `ctx->bam_leader_state.slot`, which is sourced from pack leader snapshots over `pack_bam_ldr`.
- It does not come from the BAM batch itself.
- If pack is in a no-leader gap, BAM can still log an older `current_leader_slot` until a newer pack leader snapshot arrives.

## Reading slot-timing logs

- `first_rx_minus_slot_end_ns > 0` means the first observed txn for that slot arrived after slot end when slot-end timing is known.
- `txns_before_slot_end`, `txns_after_slot_end`, and `txns_unknown_slot_end` are per-slot counters, not per-batch counters.
- If slot-end timing is unknown, BAM falls back to the coarser slot-number test when deciding whether a txn is late.
- A `current_leader_slot` equal to `max_schedule_slot` does not prove the txn was on time; same-slot late arrivals are possible when the receive timestamp is already past slot end.
