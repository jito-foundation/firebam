# FireBAM agent guidance

FireBAM integrates the BAM validator client into Firedancer.
Keep this file focused on branch-specific decisions; consult references when
the task needs them.

## Upstream Firedancer guidance

For implementation, code style, builds, generated files, and validation, follow
[CLAUDE.md](CLAUDE.md), the tracked
[upstream Firedancer guidance](https://github.com/firedancer-io/firedancer/blob/main/CLAUDE.md);
it applies to Codex too. Keep shared guidance there rather than copying it here.
Explicit user instructions and the FireBAM additions here take precedence.

## Sources by task

Live `src/` code is authoritative for this branch. Prefer code over prose when
they disagree, and report relevant discrepancies.

- For BAM tile wiring, execution, feedback, or ownership changes, start with
  `src/disco/bam/` and `src/app/firedancer/topology.c`.
  [FireBAM coordination](doc/firebam-coordination.md) explains the paths, link
  roles, and the sibling `../bam` and `../jito-solana` checkouts used for
  cross-repository comparisons.
- For the wire contract, use [bam_spec.md](bam_spec.md) and the tracked
  `src/disco/bam/proto/bam-protos` schema submodule. The spec records its verified
  revisions; recheck changing limits and behavior against live code.

## BAM invariants

- Leader snapshots on `pack_bam_ldr` are latest-value-wins. Results from
  `pack_bam_res`, `bank_bam`, and `poh_bam` form a durable FIFO across ordinary
  reconnects within a scheduler generation. Committed execution feedback
  remains provisional until PoH/motor accepts the microblock.
- Preserve `revert_on_error` atomicity and the `bam_status`/`bam_gen` ownership
  handshake when changing scheduling or mode transitions.
- When inbound limits change, compare `src/disco/bam/fd_bam_types.h` with
  `../bam/scheduler/src/types.rs`. `FD_BAM_VERIFY_OUT_DEPTH` is independent of
  `tiles.verify.receive_buffer_size` and must fit a maximum decoded response.
- `FD_BAM_RESULTS_PER_MESSAGE` must not exceed the node's
  `MAX_RESULTS_PER_SCHEDULER_MESSAGE` (`../bam/node/src/validator_service.rs`).
  It and pending-result depth describe outbound batching and queueing, not
  inbound scheduler limits. Keep fast-changing performance values in code.

## Focused workflows

Consult the matching workflow only when the task calls for it.

| Task | Reference |
| --- | --- |
| Ingress ownership, BAM health, reconnects, or Block Engine fallback | [Ownership and health](doc/firebam-coordination.md#ownership-and-health) |
| BAM protocol conformance audits or discrepancies with spec/reference behavior | [BAM spec audit skill](skills/firebam-bam-spec-audit/SKILL.md) |
| Migration to or audit against upstream main | [Upstream rebase audit skill](skills/firebam-upstream-rebase-audit/SKILL.md) |
| Versioned upstream release upgrade or audit | [Release rebase skill](skills/firedancer-release-rebase/SKILL.md) |
| BAM test targets or local suite setup | [FireBAM testing](doc/firebam-coordination.md#testing) and [upstream testing](doc/testing.md) |

## Completion

Reversible local implementation work, including builds and affected unit tests
when warranted, is authorized without per-step approval. Host setup and
integration tests follow the task's existing authorization. Preserve unrelated
work; report changes, checks, and remaining limitations.
