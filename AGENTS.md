# FireBAM agent guidance

FireBAM integrates the BAM validator client into Firedancer and Frankendancer.
Keep this file focused on branch-specific decisions; consult references when
the task needs them.

## Upstream Firedancer guidance

For Firedancer implementation, code style, builds, generated files, and
validation, follow the relevant sections of [CLAUDE.md](CLAUDE.md). This is the
[upstream Firedancer guidance](https://github.com/firedancer-io/firedancer/blob/main/CLAUDE.md)
tracked in this checkout and applies to Codex too. Keep shared guidance there
rather than copying it here.

Explicit user instructions and the FireBAM additions here take precedence over
general upstream defaults. In particular, BAM changes can require both full
Firedancer and Frankendancer paths: include `src/app/fdctl/`, `src/discoh/`, and
`agave/` when affected, despite upstream's default focus on full Firedancer.

## Sources by task

Live `src/` code is authoritative for this branch. Prefer code over prose when
they disagree, and report relevant discrepancies.

- For BAM tile wiring, execution, feedback, or ownership changes, start with
  `src/disco/bam/` and the affected topology:
  `src/app/firedancer/topology.c` (execle/poh or motor) or
  `src/app/fdctl/topology.c` (bank/pohh).
  [FireBAM coordination](doc/firebam-coordination.md) explains the paths and link roles.
- For the wire contract, use [bam_spec.md](bam_spec.md) and the tracked
  `src/disco/bam/proto/bam-protos` schema submodule. The spec records its verified
  revisions; recheck changing limits and behavior against live code.
- For cross-repository comparisons, use sibling checkouts when present:
  `../bam/AGENTS.md` and relevant BAM-node code; `../jito-solana/` supplies the
  reference validator. The local `agave/` is the Frankendancer runtime
  dependency, not the full BAM reference client.

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
- `FD_BAM_RESULTS_PER_MESSAGE` and pending-result depth describe outbound
  batching and queueing, not inbound scheduler limits. Keep fast-changing
  performance values in code.

## Focused workflows

Consult the matching workflow only when the task calls for it.

| Task | Reference |
| --- | --- |
| Ingress ownership, BAM health, reconnects, or Block Engine fallback | [Coordination skill](skills/firebam-coordination/SKILL.md) |
| BAM protocol conformance audits or discrepancies with spec/reference behavior | [BAM spec audit skill](skills/firebam-bam-spec-audit/SKILL.md) |
| Migration to or audit against upstream main | [Upstream rebase audit skill](skills/firebam-upstream-rebase-audit/SKILL.md) |
| Versioned upstream release upgrade or audit | [Release rebase skill](skills/firedancer-release-rebase/SKILL.md) |
| BAM test targets or local suite setup | [FireBAM testing](doc/firebam-coordination.md#testing) and [upstream testing](doc/testing.md) |

## Completion

Reversible local implementation work, including builds and affected unit tests
when warranted, is authorized without per-step approval. Host setup and
integration tests follow the task's existing authorization. Preserve unrelated
work; report changes, checks, and remaining limitations.
