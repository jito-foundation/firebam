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
    pack tile ──pack_bam_leader──▶ bam tile
    pack tile ──pack_bam_result──▶ bam tile       bank tile ──bank_bam─────▶ bam tile
    bam tile ──bam_status fseq──▶ verify tiles (suppresses QUIC/bundle while BAM owns TPU)

  Semantics
    pack_bam_leader carries latest-value-wins `fd_bam_leader_state_t` snapshots.
    pack_bam_result and bank_bam carry durable FIFO `fd_bam_bundle_result_t` feedback.
```
