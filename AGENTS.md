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
Firedancer Tile Flow Diagram

  TPU QUIC port (tiles.quic.quic_transaction_listen_port – default 9007)
          │
          ▼
  ┌────────────┐ net_quic  ┌───────────┐ quic_verify ┌──────────────┐ verify_dedup ┌────────────┐ dedup_resolv ┌─────────────┐ resolv_pack ┌──────────┐ pack_bank ┌────────┐
  │ net tile(s)├──────────▶│ quic tile │────────────▶│ verify tiles │─────────────▶│ dedup tile │─────────────▶│ resolv tile │────────────▶│ pack tile│──────────▶│ bank   │
  └────────────┘           └───────────┘             └──────────────┘              └────────────┘              └─────────────┘             └──────────┘           └────────┘
                                                                    ▲ bundle_verif / bam_verif fan-in
                                                                    │
   bundle gRPC/TLS feed ──▶ bundle tile ────────────────────────────┤
   BAM gRPC feed ─────────▶ bam tile ───────────────────────────────┘

  Feedback/control links
    pack tile ──pack_bundle──▶ bundle tile         bank tile ──bank_bundle──▶ bundle tile
    pack tile ──pack_bam_ldr──▶ bam tile
    pack tile ──pack_bam_res──▶ bam tile         bank tile ──bank_bam─────▶ bam tile
    bam tile ──bam_status fseq──▶ verify tiles (suppresses QUIC/bundle while BAM owns TPU)

  Semantics
    pack_bam_ldr carries latest-value-wins `fd_bam_leader_state_t` snapshots.
    pack_bam_res and bank_bam carry durable FIFO `fd_bam_bundle_result_t` feedback.
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
