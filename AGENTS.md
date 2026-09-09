Purpose: Outline Firedancer and BAM coordination in this branch.

**Source Order**
- Treat live FireBAM code in `src/`, especially `src/disco/bam/` and the two topology files below, as authoritative for this branch.
- Use the tracked `bam_spec.md` as the clean-room wire-contract baseline. It records the commits it was verified against, so recheck changing limits and behavior against live code before relying on them.
- The wire schema is the tracked `src/disco/bam/proto/bam-protos` submodule.
- When the sibling checkouts are present, use `../bam/AGENTS.md` and `../bam/docs/bam-cleanroom-spec.md` for current BAM-node context, and `../jito-solana/` for the reference validator implementation. Prefer code over prose when they disagree.

**Firedancer Core Validator (`./src`)**
- Implements the BAM validator client while maintaining Solana consensus, TPU processing, PoH, and block production.
- Interfaces: TPU pipelines, an authenticated scheduler gRPC stream to the BAM node, runtime mode control, contact-info handoff, shred routing, and durable result feedback.
- Key references: `bam_spec.md`, `src/disco/bam/`, `src/app/firedancer/topology.c`, and `src/app/fdctl/topology.c`.

**BAM Node (sibling checkout `../bam`)**
- Ingests individual TPU transactions and Block Engine packets/bundles, validates and prioritizes them, runs leader-aware auctions with speculative scheduling, and forwards ordered atomic batches to validators.
- Interfaces: QUIC TPU ingestion, authenticated gRPC validator streams, Block Engine streams, and deployment/configuration tooling. Production deployments can run inside an SEV-SNP confidential VM and use virtio-9P configuration exchange.
- Key references: `../bam/AGENTS.md`, `../bam/docs/bam-cleanroom-spec.md`; logic in `../bam/node`, `../bam/scheduler`, `../bam/state-machine`, `../bam/core`, and `../bam/api`.

**BAM Reference Validator (sibling checkout `../jito-solana`)**
- Provides the reference validator behavior used when implementing and comparing FireBAM.
- Extends Jito-Solana to receive and schedule BAM atomic batches, use Agave's transaction scheduler, and enforce `revert_on_error` semantics.
- Interfaces: scheduler gRPC stream, TPU execution pipeline, and admin RPC `setBamUrl` controlling Normal/Block-Engine/BAM modes.
- Key code: `../jito-solana/core/src/bam_connection.rs`, `../jito-solana/core/src/bam_manager.rs`, `../jito-solana/core/src/banking_stage/transaction_scheduler/`, and `../jito-solana/validator/src/commands/bam/`.
- `./agave` is the Frankendancer runtime dependency. It carries the runtime contact-info client-ID integration but is not the full BAM reference client.

**Desired Coordination Flow**
- Ingest: BAM node collects QUIC transactions and Block Engine bundles while tracking validator leader state.
- Schedule: The leader-aware auction ranks work and forwards ordered atomic batches over gRPC to the Firedancer BAM tile.
- Execute: The BAM tile feeds non-revert single transactions and `revert_on_error` atomic bundles into execution workers; Firedancer processes them in slot order.
- Feedback/control: Pack publishes latest-value-wins leader snapshots and pack, execution, and PoH publish durable results into the BAM tile. The BAM tile forwards both over the existing scheduler gRPC stream; runtime mode switches coordinate packet ownership, gossip contact information, and shred routing.

```
Current Full Firedancer Tile Flow (src/app/firedancer/topology.c)

  TPU QUIC port (tiles.quic.quic_transaction_listen_port - default 9007)
          |
          v
  net/sock --net_quic--> quic --quic_verify--> verify --verify_dedup--> dedup --dedup_resolv--> resolv --resolv_pack--> pack --pack_execle--> execle --execle_poh--> poh --poh_shred--> shred --shred_net--> net/sock
                                                        ^                                                                                 |
                                                        |                                                                                 +--pack_poh--> poh
   bundle gRPC/HTTP2 feed (optional TLS) -- bundle tile --bundle_verif---+

  Full Firedancer feedback/control links
    execle tile --execle_busy fseq--> pack tile
    repair tile --rnonce_ss shared object--> shred tile
    bundle tile --bundle_sign--> sign tile --sign_bundle--> bundle tile
    pack tile --pack_sign--> sign tile --sign_pack--> pack tile

  BAM scheduler gRPC/HTTP2 feed (TLS for HTTPS) --> bam tile --bam_verif--> verify --verify_dedup--> dedup --dedup_resolv--> resolv --resolv_pack--> pack --pack_execle--> execle --execle_poh--> poh

  Current fdctl/Frankendancer BAM Overlay (src/app/fdctl/topology.c)

  BAM scheduler gRPC/HTTP2 feed (TLS for HTTPS) --> bam tile --bam_verif--> verify --verify_dedup--> dedup --dedup_resolh--> resolh --resolh_pack--> pack --pack_bank--> bank --bank_pohh--> pohh --pohh_shred--> shred

  BAM feedback/control links
    pack tile --pack_bam_ldr--> bam tile
    pack tile --pack_bam_res--> bam tile
    bank/execle tiles --bank_bam--> bam tile
    poh/pohh tile --poh_bam--> bam tile
    replay tile (full, leader enabled) / pohh tile (fdctl) --replay_out--> bam tile
    bam tile --bam_gossip--> gossip tile (full Firedancer only)
    bam tile --bam_shred--> shred tile
    bam tile --bam_plugi--> plugin tile (fdctl only, when plugins are enabled)
    shared bam_status fseq: BAM and optional bundle write; pack reads
    shared bam_gen fseq: BAM and pack perform the ownership request/ack handshake
    bam tile <-> bam_ctrl shared object; bam tile --bam_fee_cfg--> pack tile
    bam tile --bam_sign--> sign tile --sign_bam--> bam tile

  Semantics
    Full Firedancer instantiates the BAM tile and its links on the execle/poh path.
    fdctl/Frankendancer instantiates the BAM overlay on the bank/pohh path.

  BAM feedback/control link roles
    pack_bam_ldr: Pack publishes `fd_bam_leader_state_t` snapshots with slot, tick,
      remaining CU budget, slot end time, and whether the current slot has BAM work.
      BAM treats this as latest-value-wins and sends the newest live snapshot upstream.
    pack_bam_res: Pack publishes `fd_bam_bundle_result_t` scheduling/assembly feedback
      for BAM batches, including single-transaction batches and pack-side rejection
      results. BAM queues these as durable FIFO results across scheduler stream reconnects.
    bank_bam: Bank tiles (fdctl/Frankendancer) and execle tiles (full Firedancer) publish
      immediate terminal execution failures. Successful execution remains provisional and
      travels with the microblock to PoH. Verify marks BAM parse/signature failures as
      `preprocess_failed`; dedup and resolv propagate them to pack, which publishes the
      terminal result on pack_bam_res.
    poh_bam: PoH/pohh resolves provisional execution success after accepting the microblock,
      or reports a retryable `POH_TIMEOUT` when the carrying microblock is stale or abandoned.
      BAM merges pack_bam_res, bank_bam, and poh_bam into one durable FIFO result stream.
    replay_out: Replay (full Firedancer) or pohh (fdctl) publishes reset, next-leader, and
      completed-slot hints. BAM uses them to refresh its leader-schedule gate; this is an
      unreliable latest-progress input, not durable result feedback.
    bam_gossip: In full Firedancer, BAM publishes TPU, TPU-forward, and contact-info client-ID
      changes directly to gossip and waits for the handoff before activating BAM ownership.
      Frankendancer applies the equivalent changes through the Agave admin RPC.
    bam_shred: BAM publishes `fd_bam_shred_update_t` receiver-list updates. Shred tiles
      replace their BAM destinations and forward leader/retransmit shreds to those receivers
      while BAM shred forwarding is active.
    bam_plugi: BAM publishes `fd_plugin_msg_bam_update_t` status/config updates for the
      fdctl plugin/GUI path when plugin output is enabled.
    bam_status fseq: Shared ownership state. `OVERRIDE_ACTIVE` suppresses non-BAM work while
      BAM owns TPU publication; `BUNDLE_PUBLISHING` serializes activation against a Block
      Engine drain already in progress.
    bam_gen fseq: BAM requests an ownership generation by setting the low bit. Pack retires
      pending work from the prior generation and clears the bit to acknowledge the handoff
      before BAM activates or releases ownership.
    bam_ctrl: Shared admin-control object used by CLI/RPC and BAM for set/get BAM URL,
      enable/disable state, and success/error/current-status handoff.
    bam_fee_cfg: Shared fee configuration written by BAM from scheduler config and read by
      pack to apply BAM block-builder pubkey and percentage commission metadata.
    bam_sign/sign_bam: Synchronous keyguard request/response pair. BAM asks the sign tile
      to sign auth challenges with `FD_KEYGUARD_ROLE_BAM`; sign_bam returns the signature.
```

**BAM Compatibility Invariants**
- Compare inbound scheduler limits in `src/disco/bam/fd_bam_types.h` with `../bam/scheduler/src/types.rs` whenever either side changes. Both currently cap a scheduler response at eight atomic batches, with at most five transactions per atomic batch.
- `FD_BAM_VERIFY_OUT_DEPTH` is the dedicated BAM-to-verify ring depth and is independent of `tiles.verify.receive_buffer_size`; it currently has 1,024 entries and must fit one maximum decoded scheduler message.
- `FD_BAM_RESULTS_PER_MESSAGE` and the pending-result depth are FireBAM outbound batching/queueing choices, not inbound BAM protocol limits. Keep fast-changing performance values in code rather than duplicating them here.

**Testing**
- Build unit tests before running them. `make run-unit-test` does not build test executables or the automatic test manifest.
- Many unit tests require a higher locked-memory limit than the default shell limit. Raise `MEMLOCK` in the same shell before running the suite.

Unit test workflow:

```bash
sudo src/util/shmem/fd_shmem_cfg alloc 2 gigantic 0
sudo prlimit --pid $$ --memlock=-1:-1
./contrib/make-j unit-test
make run-unit-test
```

Single-test workflow:

```bash
make -j4 test_bam_tile
sudo prlimit --pid $$ --memlock=-1:-1
build/native/gcc/unit-test/test_bam_tile
```

BAM-focused build targets:

```bash
make -j4 test_bam_tile test_bam_admin_rpc test_pack_tile_bam \
  test_resolv_tile_bam test_resolh_tile_bam \
  test_fdctl_topology_bam test_firedancer_topology_bam
```

Broader local test pass:

```bash
sudo src/util/shmem/fd_shmem_cfg alloc 2 gigantic 0
sudo prlimit --pid $$ --memlock=-1:-1
FIREDANCER_CI_COMMIT=none ./contrib/make-j all integration-test fdctl firedancer
make run-unit-test
make run-script-test
make run-fuzz-test
make run-test-vectors
make run-integration-test
DUMP=../dump make run-solcap-tests
```

Notes:
- `make run-unit-test` expects `build/native/gcc/unit-test/automatic.txt` to exist, so run `./contrib/make-j unit-test` first.
- Integration tests may change system configuration.
- If `make run-unit-test` fails with `fd_numa_mlock(... ENOMEM)` or missing workspace errors, the usual cause is that `MEMLOCK` was not raised in the current shell.
