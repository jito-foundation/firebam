# FireBAM coordination and testing

BAM integration paths, feedback/control semantics, and local test setup.
[AGENTS.md](../AGENTS.md) defines source precedence; paths below are relative
to the repository root.

## Repositories

- This repository: the BAM validator client. Wire contract: [bam_spec.md](../bam_spec.md) and the [tracked wire schema](../src/disco/bam/proto/bam-protos/). Code: [src/disco/bam/](../src/disco/bam/) and the [BAM topology overlay](../src/app/firedancer/topology_bam.c).
- BAM node (sibling checkout `../bam`): ingests TPU transactions and Block Engine bundles, runs leader-aware auctions, and streams ordered atomic batches to validators. Start at `../bam/AGENTS.md`; logic lives in `node`, `scheduler`, `state-machine`, `core`, and `api`.
- Reference validator (sibling checkout `../jito-solana`): Jito-Solana's BAM client. Key code: `core/src/bam_connection.rs`, `core/src/bam_manager.rs`, `core/src/banking_stage/transaction_scheduler/`, and `validator/src/commands/bam/`.
- Frankendancer (`fdctl`/`fddev`, `src/discoh/`, `./agave`) is upstream and has no BAM support; its config validation rejects `[tiles.bam].enabled`.

## Coordination flow

- Ingest: BAM node collects QUIC transactions and Block Engine bundles while tracking validator leader state.
- Schedule: The leader-aware auction ranks work and forwards ordered atomic batches over gRPC to the Firedancer BAM tile.
- Execute: The BAM tile sends batches through verify, dedup, and resolv to pack. Non-revert batches use ordinary transaction execution; `revert_on_error` batches use atomic bundle execution. Pack can bypass blocked work with an independent successor; PoH orders dispatched microblocks by `pack_idx` within the leader slot.
- Feedback/control: Pack publishes latest-value-wins leader snapshots and pack, execution, and PoH publish durable results into the BAM tile. The BAM tile forwards both over the existing scheduler gRPC stream; runtime mode switches coordinate packet ownership, gossip contact information, and shred routing.

The full Firedancer production path requires
`[layout].enable_block_production`; BAM additionally requires
`[tiles.bam].enabled` at startup. Runtime enable/disable operates on this
existing topology. The diagrams show the classic PoH path; full Firedancer's Alpenglow
mode substitutes `motor` for `poh`, retaining the link names below.

```
Full Firedancer (src/app/firedancer/topology.c)

  BAM scheduler gRPC/HTTP2 feed (TLS for HTTPS) --> bam --bam_verif--> verify
  verify --verify_dedup--> dedup --dedup_resolv--> resolv --resolv_pack--> pack
  pack --pack_execle--> execle --execle_poh--> poh --poh_shred--> shred
  poh tile --executed_txn--> pack tile
  pack tile --pack_sign--> sign tile --sign_pack--> pack tile (tip crank)

BAM feedback/control links
  pack tile --pack_bam_ldr--> bam tile
  pack tile --pack_bam_res--> bam tile
  execle tiles --bank_bam--> bam tile
  poh tile --poh_bam--> bam tile
  replay tile (leader enabled) --replay_slot--> bam tile
  bam tile --bam_gossip--> gossip tile
  shared bam_gossip_signed fseq: gossip acknowledges signed contact installation;
    BAM reads this and the bam_gossip consumption fseq
  bam tile --bam_shred--> shred tile
  shared bam_status fseq: BAM and optional bundle read/write; pack reads
  shared bam_gen fseq: BAM and pack perform the ownership request/ack handshake
  shared bam_ctrl: CLI and BAM exchange runtime configuration
  shared bam_fee_cfg: BAM writes; pack reads
  bam tile --bam_sign--> sign tile --sign_bam--> bam tile
```

## BAM feedback and control roles

The topology overlays linked above define link depths, consumer modes, and
configuration gates. `bam_verif` feeds verify tile 0 only.

- `executed_txn`: Full Firedancer's PoH/motor publishes signature completion
  events for BAM transactions only. Landed and completed-unlanded events both
  retire scheduled tracking in pack; only landed events delete the signature
  from pack and retire duplicate pending BAM work. Dedup does not consume the
  link (BAM bypasses its signature cache).
- `pack_bam_ldr`: Pack publishes `fd_bam_leader_state_t` with slot, tick,
  remaining CU budget, slot end time, and the current-slot BAM-work flag. BAM
  keeps the latest unsent snapshot and sends slot, tick, and remaining CU budget
  upstream, suppressing it once the slot has ended; the work flag only feeds
  BAM's health metrics. Pack publishes slot/work changes at once and other
  changes at most every 5 ms. The internal link is unreliable so old snapshots
  do not backpressure pack; BAM drops pending snapshots on reset.
- `pack_bam_res`: Pack publishes `fd_bam_bundle_result_t` scheduling,
  assembly, and rejection outcomes for atomic and non-revert batches.
- `bank_bam`: Execle publishes terminal not-committed batch outcomes
  directly. Committed batch feedback travels with
  the microblock for PoH acceptance, including non-revert transactions with
  instruction errors or fees-only outcomes. The batch `execution_success`
  field denotes commitment; per-transaction execution may still have failed.
  Verify marks BAM parse/signature failures as `preprocess_failed`; dedup and
  resolv preserve rejection markers for pack to report on `pack_bam_res`.
- `poh_bam`: PoH (or motor) publishes provisional committed feedback after
  accepting the microblock. BAM merges the three result links, and its own
  decode-time rejections, into the FIFO described below.
- `replay_slot`: Replay supplies reset and completed-slot progress. BAM
  consumes those event types to update next-leader and epoch/recheck hints for
  its connection gate and ignores other message types. This input is unreliable and polled;
  it carries progress rather than results.
- `bam_gossip`: BAM publishes `fd_bam_contact_update_t` with base
  TPU/TPU-forward sockets and contact-info client ID. Gossip also advertises
  their QUIC counterparts at base port + 6. `bam_gossip_signed` acknowledges
  installation in local CRDS of a signed ContactInfo from this instance
  advertising the requested endpoints and client ID; it is not remote
  gossip-peer acknowledgement. See [ownership and health](#ownership-and-health) for the
  activation gate.
- `bam_shred`: BAM publishes `fd_bam_shred_update_t` receiver-list updates;
  shred replaces its BAM destinations after validating the
  `FD_BAM_STEM_SIG_SHRED_UPDATE` signal and message size. Locally produced shreds
  are forwarded when this validator is the slot leader. A retransmitted shred
  is forwarded if a local leader rotation starts one to four slots after that
  shred's slot; shreds older than the current local leader-shredding slot stop
  being forwarded. Deactivation publishes an empty receiver list.
- `bam_status`: BAM and bundle coordinate mutually exclusive
  `OVERRIDE_ACTIVE` and `BUNDLE_PUBLISHING` states; pack reads ownership.
  Activation resets the direct Block Engine connection and clears bundle's
  pending queue. A publication already holding `BUNDLE_PUBLISHING` finishes
  before BAM can claim ownership.
- `bam_gen`: BAM requests a new ownership generation with the low bit set.
  Pack retires partial and pending prior-generation BAM work and clears the
  bit. Already-dispatched work remains tracked through completion.
- `bam_ctrl`: The `get-bam` and `set-bam` CLI commands exchange URL, TLS SNI,
  runtime enable state, and request success/error with BAM.
- `bam_fee_cfg`: A shared seqlock-protected snapshot, written by BAM from
  scheduler config and read by pack for builder pubkey/percentage commission.
- `bam_sign` / `sign_bam`: Synchronous keyguard request/response links for
  signing authentication challenges with `FD_KEYGUARD_ROLE_BAM`.

## Ownership and health

`CONNECTED_HEALTHY` requires a usable TCP/HTTP2/gRPC session, a live scheduler
stream, a successfully received config response, a valid (nonzero) builder
pubkey from this session, keepalive health, and a recent `BuilderHeartBeat`.
Opening the stream starts its watchdog, but receiving the builder heartbeat
establishes health; protocol `Ping` and scheduled work do not refresh it. Reset
clears config-received, builder and heartbeat state, so retained builder
metadata cannot substitute for fetching config again. As in jito-solana, a
builder update applies as a whole: an out-of-range commission or an undecodable
or all-zero builder pubkey leaves the prior builder and commission in place.

Healthy status permits activation. BAM waits for pack's generation
acknowledgement, publication of the desired shred receiver list, and completion
of any required contact-info handoff before claiming `OVERRIDE_ACTIVE`.
When switching to usable cached BAM contact endpoints, BAM waits for both the
`bam_gossip` consumer fseq and `bam_gossip_signed` to reach the handoff
target. Otherwise ownership activates with default contact information.

Pack then schedules only BAM work and simple vote transactions. Like
jito-solana's fetch stage while it advertises BAM's TPU, pack drops non-vote
transactions not from BAM, and direct Block Engine bundles, on arrival, even
when ownership activated with default contact information (where jito-solana
would keep its own TPU). On activation pack deletes pending direct Block Engine
bundles, as jito-solana clears its bundle storage; pending TPU transactions
remain. QUIC and verify do not read this ownership fseq: contact advertising
redirects ingress, while pack enforces the scheduling mode.

Reset requests deactivation immediately. An active ownership bit remains set
until pack retires pending work, and is released before the default gossip
contact is published. Bundle can reconnect once ownership is
released, including while BAM is configured but not healthy.

Because the direct Block Engine client runs until BAM is healthy, startup and
reconnects can leave it competing with the BAM node's own Block Engine
subscription. Both authenticate as this validator, and the Block Engine keeps
only the newest stream per pubkey, ending the older one with
`RESOURCE_EXHAUSTED`.

Check [fd_bam_tile.c](../src/disco/bam/fd_bam_tile.c),
[fd_bam_client.c](../src/disco/bam/fd_bam_client.c),
[bundle BAM handling](../src/disco/bundle/fd_bundle_tile_bam.h), and
[gossip BAM handling](../src/discof/gossip/fd_gossip_tile_bam.h) when changing
these transitions. Shred forwarding is implemented in
[fd_shred_tile_bam.c](../src/disco/shred/fd_shred_tile_bam.c).

## Feedback lifetime

Durability here means unsent results remain in an in-memory FIFO across ordinary
transport reconnects and client resets within the current scheduler generation.
Results enter in consumption order across producers, rather than scheduler
dispatch order. Queued results with the same `seq_id`, slot, and scheduler
generation coalesce, retaining the first queued result. BAM's FIFO holds at
most `FD_BAM_MAX_PENDING_RESULTS`. Pack's pending-result queue is also bounded;
overflow drops new results. BAM counts full-queue and stale-generation drops.
BAM consumes `pack_bam_res`, `bank_bam`, and `poh_bam` unreliably, so a stalled
BAM tile never backpressures pack, execution, or PoH; as in jito-solana's
non-blocking result send, results overrun on those links are dropped and
counted in BAM's link overrun metrics.

Identity or effective scheduler endpoint/SNI changes, and runtime disabling or
clearing the BAM URL, advance the scheduler generation and discard queued
feedback. Later results from older generations are dropped. Ordinary ownership
retirement preserves current-generation feedback. A repeated scheduler-stream
`PERMISSION_DENIED` with the same message while no next leader slot is known
can also drop the FIFO.

After a received builder heartbeat establishes stream acceptance, the client
flushes pending results in batches. A successful enqueue into the local gRPC
transmit buffer removes those results from the FIFO. There is no per-result
remote acknowledgement or persistence across process restarts. See
[fd_bam_tile_private.h](../src/disco/bam/fd_bam_tile_private.h),
[fd_bam_client.c](../src/disco/bam/fd_bam_client.c), and
[fd_pack_tile_bam.c](../src/disco/pack/fd_pack_tile_bam.c) for queue/drop boundaries.

## Testing

See [CLAUDE.md](../CLAUDE.md#validation) and [testing.md](testing.md) for
prerequisites. BAM tests are named `test_*_bam`, `test_bam_*`, and
`fuzz_bam_*`; the core targets are:

```bash
make -j4 test_bam_tile test_pack_bam test_pack_tile_bam test_execle_tile_bam \
  test_resolv_tile_bam test_firedancer_topology_bam
"$(make --silent objdir)/unit-test/test_bam_tile"   # normal pages by default
```

Changes to shared tiles and headers must still compile Frankendancer: add
`fdctl` to the build targets, since `all` does not build its Rust-linked
validator binary. BAM node
counterparts run from `../bam` with
`cargo test -p bam-node validator_service::tests::<name>`. The stateful
pipeline fuzzer is described in
[bam-pipeline-stateful-fuzz-harness.md](bam-pipeline-stateful-fuzz-harness.md).
