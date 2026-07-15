Purpose: Outline Firedancer and BAM coordination in this branch.

**Firedancer Core Validator (`./src`)**
- Maintains Solana consensus/TPU execution while exposing BAM-mode entry points noted in BAM_docs.md.
- Interfaces: TPU pipelines, gRPC handoff from BAM client, QUIC compatibility described in BAM_docs.md.
- Our goal is to add BAM support to Firedancer, so it will be a full BAM client, like in `./bam-client`
- Key references: BAM_docs.md; sources in `src/`.

**BAM Node (`./bam`)**
- Validates, filters, and sequences bundles via sealed batch auctions inside the SEV-SNP TEE.
- Interfaces: QUIC TPU ingestion, gRPC validator links, virtio-9P config exchange, Block Engine streams.
- Key refs: BAM_Node_Spec.md; logic in `bam/node`, `bam/core`, `bam/rpc`.

**BAM Client Validator Fork (`./bam-client`)**
- This is a reference client implementation for building BAM support in Firedancer
- Extends Jito-Solana to buffer BAM bundles, schedule with Agave PriorityGraph, and enforce `revert_on_error` semantics.
- Interfaces: gRPC stream from BAM node, TPU execution pipeline, admin RPC `setBamUrl` controlling Normal/Block-Engine/BAM modes.
- Key refs: BAM_Validator_Spec.md; modules in `bam-client/`.

**Desired Coordination Flow**
- Ingest: BAM node collects QUIC transactions and Block Engine bundles while tracking validator leader state.
- Schedule: Sealed auction in SEV-SNP TEE ranks bundles and forwards ordered plans over gRPC to Firedancer BAM Tile.
- Execute: BAM Tile feeds bundles into execution workers; Firedancer processes them in slot order.
- Feedback/control: Pack publishes latest-value-wins leader snapshots, and pack/bank publish durable bundle results, into the BAM tile. The BAM tile forwards both over the existing scheduler gRPC stream; mode switches follow runtime selection in BAM_Validator_Spec.md.

```
Current Full Firedancer Tile Flow (src/app/firedancer/topology.c)

  TPU QUIC port (tiles.quic.quic_transaction_listen_port - default 9007)
          |
          v
  net/sock --net_quic--> quic --quic_verify--> verify --verify_dedup--> dedup --dedup_resolv--> resolv --resolv_pack--> pack --pack_execle--> execle --execle_poh--> poh --poh_shred--> shred --shred_net--> net/sock
                                                        ^                                                                                 |
                                                        |                                                                                 +--pack_poh--> poh
   bundle gRPC/TLS feed -- bundle tile --bundle_verif---+

  Full Firedancer feedback/control links
    execle tile --execle_busy fseq--> pack tile
    repair tile --rnonce_ss shared object--> shred tile
    bundle tile --bundle_sign--> sign tile --sign_bundle--> bundle tile
    pack tile --pack_sign--> sign tile --sign_pack--> pack tile

  Current fdctl/Frankendancer BAM Overlay (src/app/fdctl/topology.c only)

  BAM scheduler gRPC/TLS feed --> bam tile --bam_verif--> verify --verify_dedup--> dedup --dedup_resolh--> resolh --resolh_pack--> pack --pack_bank--> bank --bank_pohh--> pohh --pohh_shred--> shred

  BAM feedback/control links
    pack tile --pack_bam_ldr--> bam tile
    pack tile --pack_bam_res--> bam tile
    bank/verify tiles --bank_bam--> bam tile
    bam tile --bam_shred--> shred tile
    bam tile --bam_plugi--> plugin tile (plugin/GUI enabled)
    bam tile --bam_status fseq--> pack/bundle tiles
    bam tile <-> bam_ctrl shared object; bam tile --bam_fee_cfg--> pack tile
    bam tile --bam_sign--> sign tile --sign_bam--> bam tile

  Semantics
    The full Firedancer topology does not currently instantiate a BAM tile or BAM links.
    The BAM overlay above is currently wired in fdctl/Frankendancer and should be ported onto
    the full Firedancer execle/poh path.

  BAM feedback/control link roles
    pack_bam_ldr: Pack publishes `fd_bam_leader_state_t` snapshots with slot, tick,
      remaining CU budget, slot end time, and whether the current slot has BAM work.
      BAM treats this as latest-value-wins and sends the newest live snapshot upstream.
    pack_bam_res: Pack publishes `fd_bam_bundle_result_t` scheduling/assembly feedback
      for BAM bundles, including pack-side rejection results. BAM queues these as durable
      FIFO results across scheduler stream reconnects.
    bank_bam: Bank tiles publish executed `fd_bam_bundle_result_t` outcomes, while verify
      tiles publish parse/signature failure results for BAM transactions. BAM merges all
      bank_bam producers into the same durable FIFO result stream as pack_bam_res.
    bam_shred: BAM publishes `fd_bam_shred_update_t` receiver-list updates. Shred tiles
      replace their BAM destinations and forward leader/retransmit shreds to those receivers
      while BAM shred forwarding is active.
    bam_plugi: BAM publishes `fd_plugin_msg_bam_update_t` status/config updates for the
      plugin/GUI path when plugin output is enabled.
    bam_status fseq: BAM writes the override-active bit. Pack and bundle tiles consult
      this latch to suppress non-BAM work while BAM owns TPU work.
    bam_ctrl: Shared admin-control object used by CLI/RPC and BAM for set/get BAM URL,
      enable/disable state, and success/error/current-status handoff.
    bam_fee_cfg: Shared fee configuration written by BAM from scheduler config and read by
      pack to apply BAM priority-fee recipient and commission metadata.
    bam_sign/sign_bam: Synchronous keyguard request/response pair. BAM asks the sign tile
      to sign auth challenges with `FD_KEYGUARD_ROLE_BAM`; sign_bam returns the signature.
```

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
