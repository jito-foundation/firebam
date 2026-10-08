# Tile Communication Map

This map is based on the topology builders and tile init paths in:

- [src/disco/topo/fd_topo.h](../src/disco/topo/fd_topo.h)
- [src/disco/topo/fd_topob.c](../src/disco/topo/fd_topob.c)
- [src/disco/net/fd_net_tile_topo.c](../src/disco/net/fd_net_tile_topo.c)
- [src/app/firedancer/topology.c](../src/app/firedancer/topology.c)
- [src/app/fdctl/topology.c](../src/app/fdctl/topology.c)
- BAM, bundle, verify, quic, pack, sign, shred, gossip, resolv, execle, plugin tile sources under `src/disco/`, `src/discof/`, and `src/discoh/`

Depths, MTUs, gates, and consumer modes below use the live source expressions rather than duplicated numeric values where the code defines a named constant. The [companion diagram](tile-communication.mmd) shows selected paths, groups repeated tile/link instances, and combines conditional configurations; it is not a single runnable topology. The tables identify the gates and consumer modes.

There are two production source topologies in this branch:

- Full Firedancer: `src/app/firedancer/topology.c`. Its leader execution path is `quic -> verify -> dedup -> resolv -> pack -> execle -> poh/motor -> shred`. It has bundle support and, when `leader_enabled && config->tiles.bam.enabled`, instantiates the BAM tile and BAM overlay links on the execle/PoH-or-motor path.
- Frankendancer/fdctl: `src/app/fdctl/topology.c`. Its execution path is `quic -> verify -> dedup -> resolh -> pack -> bank -> pohh -> shred`. It also wires a BAM overlay when `config->tiles.bam.enabled`, but its result producers and plugin path differ from full Firedancer.

This map describes the source wiring, including conditional edges. Two existing
configuration caveats follow from that wiring: full Firedancer telemetry's
`report_shreds` tap iterates `shred_tile_cnt` over `net_shred` kind IDs, so unequal
network/shred counts can leave links untapped or reference a missing kind.
fdctl always creates `bank_pack` but adds its consumer only when
`tiles.pack.use_consumed_cus` is enabled; disabling it leaves a consumerless
link without `permit_no_consumers`, conflicting with `fd_topob_finish`'s
validation in `fd_topob.c`.

## Topology Primitives

`fd_topob_link` creates a named link instance. The link kind id is the ordinal among links with the same name. Repeated links below are written as `[i]`; for example `quic_verify[i]` is kind id `i`.

Each normal link has an mcache and, when MTU is nonzero, a dcache. A link has one producer and may have many consumers. Reliability and polling are per consumer input, not per link. `FD_TOPOB_UNPOLLED` inputs are required to be unreliable and are used for synchronous side loops such as keyguard responses.

Network RX links created by `fd_topos_net_rx_link` are special:

- With XDP, their mcache is in `net_umem`, their dcache is the XDP UMEM object owned by the producer net tile, and their burst is `0`.
- With mlx5, they use the same `net_umem`/provider-UMEM arrangement and the burst is the configured mlx5 batch size.
- With socket networking, they use a normal dcache in `net_umem` and burst `64`.

Unless noted otherwise, non-network producer-to-consumer links below are reliable and polled by `fd_stem`.

## Full Firedancer Link Matrix

This section covers `src/app/firedancer/topology.c`. `leader_enabled` means
`config->firedancer.layout.enable_block_production`; `bam_enabled` also requires
`config->tiles.bam.enabled`. `tip_crank_enabled` requires leader mode and either
bundle or BAM configuration. Outside Alpenglow, the builder uses `repair`, `poh`,
`tower`, and `txsend`; Alpenglow replaces `repair`/`poh` with `rotor`/`motor` and
uses `votor` instead of tower/txsend. Link names retain the repair/PoH prefixes.

| Links | Producers | Consumers | Reliability / polling | Depth | MTU / burst | Workspace / gate | Payload and semantics |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `net_gossvf[k]`, `net_shred[k]`, `net_repair[k]`; optional `net_txsend[k]`, `net_quic[k]`, `net_rserve[k]`, `net_votor[k]` | selected network provider tiles | all gossvf/shred for their links; repair/rotor; txsend, quic, rserve, votor for their corresponding links; optional event taps `net_shred` | Unreliable/polled | `config->net.ingress_buffer_size` | `FD_NET_MTU`; XDP burst `0`, mlx5 burst `tile->mlx5.batch_size`, socket burst `64` | `net_umem`; txsend non-Alpenglow, QUIC leader-enabled, rserve enabled, votor Alpenglow | Network RX. mlx5 additionally creates loopback socket clones described below. |
| mlx5 loopback clones of provider-0 `net_*` RX links | additional `sock` tile | same consumers and input modes as provider-0 links | Unreliable/polled | `4096UL` | `FD_NET_MTU`, burst `64UL` | `sock`; mlx5 only | New same-name kind instances created by `fd_topos_sock_lo`; these are not ordinary `net_umem` RX instances. |
| `gossip_net`, `shred_net[i]` | gossip, shred[i] | selected network provider tiles | Unreliable/polled | `32768UL` | `FD_NET_MTU`, burst `1UL` | `net_gossip`, `net_shred` | Outbound packets. GUI does not tap gossip_net; it uses gossip_gui. |
| `repair_net`, `quic_net[i]`, optional `txsend_net`, `rserve_net` | repair/rotor, quic[i], txsend, rserve | selected network provider tiles; GUI also taps repair_net | Unreliable/polled | `config->net.ingress_buffer_size` | `FD_NET_MTU`, burst `1UL` | corresponding `net_*` workspace; QUIC leader-enabled, txsend non-Alpenglow, rserve enabled | Outbound packets. The mlx5 loopback socket is added after network-TX consumer wiring. |
| `votor_net` | votor | selected network provider tiles | Unreliable/polled | `32768UL` | `FD_NET_MTU`, burst `FD_VOTOR_NET_BURST` | `net_votor`; Alpenglow | Vote/certificate networking. |
| `net_netlnk[k]` | net[k] or mlx5[k] | netlnk | Unreliable/polled | `128UL` | MTU `0UL`, burst `0UL` | `net_netlnk`; XDP/mlx5 | Address-resolution/control requests. |
| `iproute_out` | netlnk | all net or mlx5 tiles | Reliable/polled | `fd_ulong_pow2_up(4UL*(netlnk_max_routes+netlnk_max_peer_routes)+8UL)` | `sizeof(fd_iproute_msg_t)`, burst `1UL` | `iproute`; XDP/mlx5 | Route dumps/updates. Created by the network helper. |
| `genesi_out` | genesi | ipecho, repair/rotor, replay; optional event, RPC, GUI | Reliable/polled | `1UL` | `fd_genesi_tile_mtu(genesis_max_message_size)`, burst `1UL` | `genesi_out` | Genesis/bootstrap data. |
| `ipecho_out` | ipecho | gossvf, gossip, shred, replay, tower/votor; optional event | Reliable/polled except votor Unreliable/polled | `2UL` | MTU `0UL`, burst `1UL` | `ipecho_out` | Shred version in the fragment signal. |
| `gossvf_gossip[i]` | gossvf[i] | gossip | Unreliable/polled | `config->net.ingress_buffer_size` | `FD_GOSSIP_GOSSVF_MTU`, burst `1UL` | `gossvf_gossip` | Verified gossip packets. |
| `gossip_gossvf` | gossip | all gossvf | Reliable/polled | `65536UL*4UL` | `sizeof(fd_gossip_ping_update_t)`, burst `1UL` | `gossip_gossvf` | Ping state updates. |
| `gossip_ciaddr` | gossip | gossvf, repair/rotor, shred; txsend outside Alpenglow or votor in Alpenglow; optional RPC and snapct | Reliable/polled | `65536UL*2UL` | `sizeof(fd_gossip_update_message_t)`, burst `1UL` | `gossip_ciaddr`; snapct consumption requires snapshots and gossip snapshot sources | Contact address updates. |
| `gossip_ciseen` | gossip | GUI | Reliable/polled | `65536UL*2UL` | `sizeof(fd_gossip_update_message_t)`, burst `1UL` | `gossip_ciseen`; GUI enabled | Contact last-seen updates. |
| `gossip_vote` | gossip | all verify tiles when leader enabled; RPC outside Alpenglow | Reliable/polled | `65536UL*4UL` | `sizeof(fd_gossip_update_message_t)`, burst `1UL` | `gossip_vote`; `gossip_vote_enabled` | Gossip vote updates. |
| `gossip_misc` | gossip | replay; tower outside Alpenglow | Reliable/polled | `65536UL*2UL` | `sizeof(fd_gossip_update_message_t)`, burst `1UL` | `gossip_misc` | Remaining gossip updates. There is no current `gossip_out` link. |
| `quic_verify[i]` | quic[i] | all verify | Unreliable/polled | `config->tiles.verify.receive_buffer_size` | `sizeof(fd_tpu_msg_t)`, burst `config->tiles.quic.txn_reassembly_count` | `quic_verify`; leader enabled | Reassembled TPU packets; verify assigns QUIC sequences round-robin. |
| `verify_dedup[i]` | verify[i] | dedup | Reliable/polled | `1024UL` | `FD_TPU_PARSED_MTU`, burst `1UL` | `verify_dedup`; leader enabled | Parsed transaction metadata/payload and BAM rejection markers. |
| `dedup_resolv` | dedup | all resolv; tower outside Alpenglow; event when reporting transactions | Reliable/polled | `16384UL` | `FD_TPU_PARSED_MTU`, burst `1UL` | `dedup_resolv`; leader enabled | Parsed transactions; BAM/grouped work is routed to resolv:0 by consumer logic. |
| `resolv_pack[i]` | resolv[i] | pack | Reliable/polled | `4096UL` | `FD_TPU_RESOLVED_MTU`, burst `1UL` | `resolv_pack`; leader enabled | Resolved transaction metadata. Current topology does not impose a single-resolv BAM restriction. |
| `resolv_replay[i]` | resolv[i] | replay | Reliable/polled | `4096UL` | `sizeof(fd_resolv_slot_exchanged_t)`, burst `1UL` | `resolv_replay`; leader enabled | Bank-slot reference exchange notifications. |
| `pack_execle[i]` | pack | execle[i]; optional GUI | Reliable/polled | `128UL` | `FD_PACK_EXECLE_MTU`, burst `1UL` | `pack_execle`; leader enabled | Scheduled transaction work. |
| `execle_pack[i]` | execle[i] | pack | Reliable/polled | `1024UL` | `FD_PACK_REBATE_MAX_SZ`, burst `1UL` | `execle_pack`; leader and use_consumed_cus | CU rebates. |
| `pack_poh` | pack | poh/motor; optional GUI | Reliable/polled | `4096UL` | `sizeof(fd_done_packing_t)`, burst `1UL` | `pack_poh`; leader enabled | Packing completion/control. |
| `execle_poh[i]` | execle[i] | poh/motor; optional GUI | Reliable/polled | `4096UL` | `FD_EXECLE_POH_MTU`, burst `1UL` | `execle_poh`; leader enabled | Executed microblocks and optional provisional committed BAM feedback. |
| `poh_shred` | poh/motor | all shred | Reliable/polled | `4096UL` | `FD_POH_SHRED_MTU`, burst `1UL` | `poh_shred`; leader enabled | Entries for shredding, not already-formed network shreds. |
| `poh_replay` | poh/motor | replay | Reliable/polled | `4096UL` | `sizeof(fd_poh_leader_slot_ended_t)`, burst `1UL` | `poh_replay`; leader enabled | Leader slot end feedback. |
| `executed_txn` | poh/motor | pack | Reliable/polled | `16384UL` | `FD_TXN_SIGNATURE_SZ`, burst `1UL` | `executed_txn`; BAM and leader enabled | BAM transaction landed and completed-unlanded events; other sources are not published. |
| `replay_out` | replay | tower outside Alpenglow; dedup and pack when leader enabled | Reliable/polled | `65536UL` | `sizeof(fd_replay_message_t)`, burst `1UL` | `replay_out`; permit_no_consumers | Replay event stream including transaction execution. |
| `replay_slot` | replay | repair/rotor, resolv and poh/motor when leader enabled, votor in Alpenglow; optional RPC and GUI; BAM and bundle when present; shred in Alpenglow | Reliable/polled except BAM/bundle/shred Unreliable/polled | `4096UL` | `sizeof(fd_replay_message_t)`, burst `1UL` | `replay_slot` | Replay event fanout excluding transaction-executed events; BAM consumes reset/completed-slot hints. |
| `replay_epoch` | replay | gossvf, gossip, shred, tower/txsend outside Alpenglow or votor in Alpenglow; optional RPC, GUI | Reliable/polled | `16UL` | `FD_EPOCH_OUT_MTU`, burst `1UL` | `replay_epoch` | Epoch and stake updates. |
| `replay_execrp[i]` | replay | execrp[i] | Reliable/polled | `256UL` | `sizeof(fd_execrp_task_msg_t)`, burst `1UL` | `replay_execrp` | Replay execution tasks. |
| `execrp_replay[i]` | execrp[i] | replay; optional GUI | Reliable/polled | `16384UL` | `sizeof(fd_execrp_task_done_msg_t)`, burst `1UL` | `execrp_replay` | Replay task completions. |
| `shred_out[i]` | shred[i] | repair/rotor; tower outside Alpenglow; optional rserve and GUI | Reliable/polled, including rserve | `shred_depth` (`65536UL`) | `sizeof(fd_shred_message_t)`, burst `FD_SHRED_STEM_BURST` | `shred_out` | FEC completion/control messages; includes received/repaired FEC sets, not just locally produced shreds. |
| `repair_out` | repair/rotor | replay | Reliable/polled | `shred_depth` (`65536UL`) | `sizeof(fd_repair_fec_complete_t)`, burst `1UL` | `repair_out` | Repair/FEC completion notifications. |
| `tower_out` | tower | repair, replay, gossip, txsend; shred; optional RPC, GUI | Reliable/polled except shred Unreliable/polled | `16384UL` | `sizeof(fd_tower_msg_t)`, burst `2UL` | `tower_out`; non-Alpenglow | Tower progress, confirmations and slot completion. |
| `votor_out` | votor | rotor, replay; optional GUI, RPC | Reliable/polled | `1024UL` | `sizeof(fd_votor_msg_t)`, burst `FD_VOTOR_OUT_BURST` | `votor_out`; Alpenglow | Alpenglow certificates/finality/progress. Shred does not consume this link. |
| `rotor_rserve` | rotor | rserve | Unreliable/polled | `2048UL` | `sizeof(fd_rotor_block_t)`, burst `1UL` | `rotor_rserve`; Alpenglow and rserve | Certified block/FEC metadata for repair server. |
| `txsend_out` | txsend | replay, gossip, verify[0] when leader enabled | Reliable/polled | `128UL` | `FD_TPU_RAW_MTU`, burst `1UL` | `txsend_out`; non-Alpenglow | Locally submitted transactions. |
| `bundle_verif` | bundle | all verify | Reliable/polled | `config->tiles.verify.receive_buffer_size` | `FD_TPU_PARSED_MTU`, burst `1UL` | `bundle_verif`; bundle and leader enabled | Bundle packets use sequence round-robin only at signal 0; nonzero bundle signals go to verify:0. All BAM fragments use verify:0 independently of signal. |
| `bundle_status` | bundle | GUI | Reliable/polled | `128UL` | `sizeof(fd_bundle_block_engine_update_t)`, burst `1UL` | `bundle_status`; bundle and GUI enabled | Block Engine status. |
| `rpc_replay` | RPC | replay | Reliable/polled | `8UL` | MTU `0UL`, burst `1UL` | `rpc_replay`; RPC enabled | RPC control/notification signal. |
| `admin_replay`, `replay_admin` | admin, replay | replay, admin respectively | Reliable/polled | `32UL` | MTU `0UL`, burst `1UL` | both in `admin_replay` | Admin request-response signals. |
| `diag_gui` | diag | GUI | Unreliable/polled | `4UL` | `sizeof(fd_diag_system_resources_t)`, burst `1UL` | `diag_gui`; GUI enabled | Resource diagnostics. |
| `gossip_gui`, `gossvf_gui[i]` | gossip, gossvf[i] | GUI | Unreliable/polled | `256UL` | `FD_GUI_GOSSIP_BW_MTU`, burst `1UL` | `gossip_gui`; GUI enabled | Gossip bandwidth samples, separate from gossip_net/ciaddr. |
| `cap_repl`, `cap_execrp[i]` | replay, execrp[i] | solcap | Reliable/polled | `32UL` | `SOLCAP_WRITE_ACCOUNT_DATA_MTU`, burst `1UL` | `solcap`; solcap enabled | Account capture data. |

## Full Firedancer BAM Overlay

This section covers BAM links in `src/app/firedancer/topology.c` when `leader_enabled && config->tiles.bam.enabled`.

| Link | Kind ids | Producer | Consumers | Reliability / polling | Depth | MTU / burst | Workspace / gate | Payload and semantics |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `bam_verif` | `0` | `bam` | `verify[0]` | Reliable, polled | `FD_BAM_VERIFY_OUT_DEPTH` | `FD_TPU_PARSED_MTU`, burst `FD_BAM_STEM_BURST` | `bam_verif`; BAM+leader enabled | BAM scheduler transactions as `fd_txn_m_t` plus payload with `source_tpu=FD_TXN_M_TPU_SOURCE_BAM`. Verify forwards valid packets and marks parse/signature failures `preprocess_failed`; pack later publishes their terminal result on `pack_bam_res`. |
| `bam_sign` | `0` | `bam` | `sign[0]` | Unreliable, polled | `128` | `256`, burst `1` | `bam_sign`; BAM+leader enabled | BAM auth challenge signing request. |
| `sign_bam` | `0` | `sign[0]` | `bam` | Unreliable, unpolled | `128` | `64`, burst `1` | `sign_bam`; BAM+leader enabled | BAM auth signature response, consumed by keyguard client spin loop. |
| `bam_gossip` | `0` | `bam` | `gossip` | Reliable, polled | `128` | `sizeof(fd_bam_contact_update_t)`, burst `1` | `bam_gossip`; full Firedancer BAM only | Contact-info update carrying BAM/default TPU, TPU-forward, and version client-id state. See [ownership and health](firebam-coordination.md#ownership-and-health) for handoff gates. |
| `pack_bam_ldr` | `0` | `pack` | `bam` | Unreliable, polled | `FD_BAM_MAX_PENDING_RESULTS` | `sizeof(fd_bam_leader_state_t)`, burst `1` | `pack_bam_ldr`; BAM+leader enabled | Leader state snapshots: slot, tick, CU budget, slot end time, and current-slot BAM work bit. BAM coalesces this as latest-value-wins. |
| `pack_bam_res` | `0` | `pack` | `bam` | Unreliable, polled | `FD_BAM_RESULT_LINK_DEPTH` | `sizeof(fd_bam_bundle_result_t)`, burst `1` | `pack_bam_res`; BAM+leader enabled | Scheduling/rejection feedback for BAM batches, including single-transaction batches. See [feedback lifetime](firebam-coordination.md#feedback-lifetime) for queue semantics. |
| `bank_bam[i]` | `0..execle_tile_cnt-1` | `execle[i]` | `bam` | Unreliable, polled | `FD_BAM_RESULT_LINK_DEPTH` | `sizeof(fd_bam_bundle_result_t)`, burst `1` | `bank_bam`; BAM+leader enabled | Immediate not-committed outcomes. Committed execution feedback remains provisional and travels with the microblock to PoH/motor. |
| `poh_bam` | `0` | `poh`/`motor` | `bam` | Unreliable, polled | `FD_BAM_RESULT_LINK_DEPTH` | `sizeof(fd_bam_bundle_result_t)`, burst `1` | `poh_bam`; BAM+leader enabled | Resolves provisional committed outcomes after accepting the microblock, or reports retryable `POH_TIMEOUT` when the carrying microblock is stale or abandoned. |
| `bam_shred` | `0` | `bam` | all `shred` tiles | Reliable, polled | `128` | `sizeof(fd_bam_shred_update_t)`, burst `1` | `bam_shred`; BAM+leader enabled | BAM shred receiver list update. Published with `FD_BAM_STEM_SIG_SHRED_UPDATE`; shred validates size and signal before replacing BAM destinations. |

The BAM tile has an external gRPC-over-HTTP/2 scheduler boundary, with TLS when the endpoint uses HTTPS. Inbound scheduler batches are converted into `bam_verif` fragments. Outbound feedback uses the same scheduler stream: `pack_bam_ldr` is sent as leader state messages, while `pack_bam_res`, `bank_bam`, and `poh_bam` feed result messages.

## Snapshot Links

Snapshot-load links require `snapshots_enabled` (`config->gossip.entrypoints_cnt`
is nonzero). Snapshot-production links require `snapmk_enabled` (a nonzero
configured compression tile count with snapshot production enabled). Server
links require configured snapshot-server tiles. Inputs are reliable and polled
except the noted GUI tap. Workspaces have the same names as the links.

| Links | Producers | Consumers | Depth | MTU / burst | Gate / semantics |
| --- | --- | --- | --- | --- | --- |
| `snapct_ld` | snapct | snapld | `128UL` | `sizeof(fd_ssctrl_msg_t)`, burst `1UL` | snapshots_enabled; load control. |
| `snapld_dc` | snapld | snapct, all snapdc | `FD_SNAPSHOT_DATA_DEPTH` | `FD_SNAPSHOT_DATA_MTU`, burst `1UL` | snapshots_enabled; downloaded data/control. |
| `snapdc_in[i]` | snapdc[i] | all snapin | `FD_SNAPSHOT_DC_IN_DEPTH` | `FD_SNAPSHOT_DATA_MTU`, burst `1UL` | snapshots_enabled; decompressed data. |
| `snapin_manif` | snapin[0] | gossip, repair/rotor, replay; optional GUI | `4UL` | `sizeof(fd_snapshot_manifest_t)`, burst `1UL` | snapshots_enabled; full/incremental/done manifest. |
| `snapct_repr` | snapct | no current consumers; zero consumers allowed | `128UL` | MTU `0UL`, burst `1UL` | snapshots_enabled; planned repair hook. |
| `snapct_gui`, `snapin_gui[i]` | snapct, snapin[i] | GUI | `128UL` | `sizeof(fd_snapct_update_t)` and `FD_GUI_CONFIG_PARSE_MAX_VALID_ACCT_SZ`, burst `1UL` | snapshots+GUI enabled. |
| `snapin_ct[i]` | snapin[i] | snapct | `128UL` | MTU `0UL`, burst `1UL` | snapshots_enabled; control return. No current snapwr tile/links. |
| `replay_snapmk` | replay | snapmk | `16UL` | `sizeof(fd_replay_snap_start_t)`, burst `1UL` | snapmk_enabled; production start. |
| `snapmk_zp[i]` | snapmk | snapzp[i] | `1024UL` | `sizeof(fd_backup_frag_t)`, burst `1UL` | snapmk_enabled; compression tasks. |
| `snaprd_out` | snaprd | snapmk | `1024UL` | `FD_BACKUP_RD_MTU`, burst `1UL` | snapmk_enabled; backup read data. snapzp also directly reads this dcache as shared memory (not a link input). |
| `snapmk_out` | snapmk | replay, all snapsv | `128UL` | `sizeof(fd_snapmk_msg_t)`, burst `1UL` | snapmk_enabled; produced-snapshot metadata. |
| `snapsv_out[i]` | snapsv[i] | optional GUI (unreliable/polled); zero consumers allowed | `16384UL` | `sizeof(fd_snapsv_msg_t)`, burst `1UL` | configured snapshot server with production; server status. |

## Keyguard / Sign Links

This table describes full Firedancer. Gossip, shred, and repair/rotor use
asynchronous polled responses. Other listed clients publish requests and read
synchronous responses out of band, so their response inputs are unreliable
and unpolled. Request inputs are polled; all signing link bursts are `1UL`,
and workspaces match each link name. fdctl has asynchronous shred signing and
synchronous bundle, pack, and BAM signing, detailed in its tables below.

| Request -> response | Producer / consumer | Role | Depth request / response | Request MTU | Response MTU | Reliability / polling | Gate |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `gossip_sign` -> `sign_gossip` | gossip <-> sign[0] | `FD_KEYGUARD_ROLE_GOSSIP` | `128UL` / `128UL` | `2048UL` | `sizeof(fd_ed25519_sig_t)` | request Unreliable/polled; response Unreliable/polled, asynchronous | Always |
| `shred_sign[i]` -> `sign_shred[i]` | shred[i] <-> sign[0] | `FD_KEYGUARD_ROLE_LEADER` | `128UL` / `128UL` | `32UL` | `sizeof(fd_ed25519_sig_t)` | request Unreliable/polled; response Unreliable/polled, asynchronous | Always |
| `repair_sign[i]` -> `sign_repair[i]` | repair/rotor <-> sign[i+1] | `FD_KEYGUARD_ROLE_REPAIR` | `256UL` / `256UL` | `FD_REPAIR_MAX_PREIMAGE_SZ` | `sizeof(fd_ed25519_sig_t)` | request Unreliable/polled; response Unreliable/polled, asynchronous | sign_tile_cnt-1 instances |
| `rserve_sign` -> `sign_rserve` | rserve <-> sign[0] | `FD_KEYGUARD_ROLE_RSERVE` | `128UL` / `128UL` | `32UL` | `sizeof(fd_ed25519_sig_t)` | request Reliable/polled; response Unreliable/unpolled | rserve_enabled |
| `txsend_sign` -> `sign_txsend` | txsend <-> sign[0] | `FD_KEYGUARD_ROLE_TXSEND` | `128UL` / `128UL` | `FD_TXN_MTU_V0` | `sizeof(fd_ed25519_sig_t)*2UL` | request Reliable/polled; response Unreliable/unpolled | non-Alpenglow |
| `bundle_sign` -> `sign_bundle` | bundle <-> sign[0] | `FD_KEYGUARD_ROLE_BUNDLE` | `65536UL` / `128UL` | `9UL` | `64UL` | request Unreliable/polled; response Unreliable/unpolled | bundle/leader enabled |
| `pack_sign` -> `sign_pack` | pack <-> sign[0] | `FD_KEYGUARD_ROLE_BUNDLE_CRANK` | `65536UL` / `128UL` | `1232UL` | `64UL` | request Unreliable/polled; response Unreliable/unpolled | tip_crank_enabled |
| `bam_sign` -> `sign_bam` | BAM <-> sign[0] | `FD_KEYGUARD_ROLE_BAM` | `128UL` / `128UL` | `256UL` | `64UL` | request Unreliable/polled; response Unreliable/unpolled | bam_enabled |
| `event_sign` -> `sign_event` | event <-> sign[0] | `FD_KEYGUARD_ROLE_EVENT` | `128UL` / `128UL` | `317UL` | `64UL` | request Unreliable/polled; response Unreliable/unpolled | telemetry_enabled |
| `votor_sign` -> `sign_votor` | votor <-> sign[0] | `FD_KEYGUARD_ROLE_VOTOR` | `128UL` / `128UL` | `130UL` | `FD_KEYGUARD_BLS_SIG_SZ` | request Reliable/polled; response Unreliable/unpolled | Alpenglow |

## Frankendancer / fdctl BAM Overlay

This section covers BAM links in `src/app/fdctl/topology.c` when `config->tiles.bam.enabled` is true. Most contracts match full Firedancer, but fdctl uses Agave bank/PoH tile names and has a plugin output path.

| Link | Kind ids | Producer | Consumers | Reliability / polling | Depth | MTU / burst | Workspace / gate | Payload and semantics |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `bam_verif` | `0` | `bam` | `verify[0]` | Reliable, polled | `FD_BAM_VERIFY_OUT_DEPTH` | `FD_TPU_PARSED_MTU`, burst `FD_BAM_STEM_BURST` | `bam_verif`; BAM enabled | BAM scheduler transactions as `fd_txn_m_t` plus payload with `source_tpu=FD_TXN_M_TPU_SOURCE_BAM`. Verify forwards valid packets and marks parse/signature failures `preprocess_failed`; pack later publishes their terminal result on `pack_bam_res`. |
| `bam_sign` | `0` | `bam` | `sign[0]` | Unreliable, polled | `128` | `256`, burst `1` | `bam_sign`; BAM enabled | BAM auth challenge signing request. |
| `sign_bam` | `0` | `sign[0]` | `bam` | Unreliable, unpolled | `128` | `64`, burst `1` | `sign_bam`; BAM enabled | BAM auth signature response, consumed by keyguard client spin loop. |
| `pack_bam_ldr` | `0` | `pack` | `bam` | Unreliable, polled | `FD_BAM_MAX_PENDING_RESULTS` | `sizeof(fd_bam_leader_state_t)`, burst `1` | `pack_bam_ldr`; BAM enabled | Leader state snapshots: slot, tick, CU budget, slot end time, and current-slot BAM work bit. BAM coalesces this as latest-value-wins; newer snapshots may supersede older ones while BAM is reconnecting or behind. |
| `pack_bam_res` | `0` | `pack` | `bam` | Unreliable, polled | `FD_BAM_RESULT_LINK_DEPTH` | `sizeof(fd_bam_bundle_result_t)`, burst `1` | `pack_bam_res`; BAM enabled | Scheduling/rejection feedback for BAM batches, including single-transaction batches. See [feedback lifetime](firebam-coordination.md#feedback-lifetime) for queue semantics. |
| `bank_bam[i]` | `0..bank_tile_cnt-1` | `bank[i]` | `bam` | Unreliable, polled | `FD_BAM_RESULT_LINK_DEPTH` | `sizeof(fd_bam_bundle_result_t)`, burst `1` | `bank_bam`; BAM enabled | Immediate not-committed outcomes. Committed execution feedback remains provisional and travels with the microblock to pohh. |
| `poh_bam` | `0` | `pohh` | `bam` | Unreliable, polled | `FD_BAM_RESULT_LINK_DEPTH` | `sizeof(fd_bam_bundle_result_t)`, burst `1` | `poh_bam`; BAM enabled | Resolves provisional committed outcomes after pohh accepts the microblock, or reports retryable `POH_TIMEOUT` for stale or abandoned microblocks. |
| `bam_shred` | `0` | `bam` | all `shred` tiles | Reliable, polled | `128` | `sizeof(fd_bam_shred_update_t)`, burst `1` | `bam_shred`; BAM enabled | BAM shred receiver list update. Published with `FD_BAM_STEM_SIG_SHRED_UPDATE`; shred validates size and signal before replacing BAM destinations. |
| `replay_out` | `0` | `pohh` | `bam` | Unreliable, polled | `128` | `sizeof(fd_replay_message_t)`, burst `1` | `replay_out`; BAM enabled in fdctl | Local leader schedule hints from the Agave PoH side. BAM consumes reset and slot-completed messages to gate/recheck scheduler connection attempts. |
| `bam_plugi` | `0` | `bam` | `plugin` | Reliable, polled | `65536` | `sizeof(fd_plugin_msg_bam_update_t)`, burst `1` | `bam_plugi`; BAM and plugin/GUI enabled | BAM status/config to plugin/GUI. |

The BAM tile also has an external gRPC-over-HTTP/2 scheduler boundary, with TLS when the endpoint uses HTTPS. Inbound scheduler batches are converted into `bam_verif` fragments. Outbound feedback uses the same scheduler stream: `pack_bam_ldr` is sent as leader state messages, while `pack_bam_res`, `bank_bam`, and `poh_bam` feed result messages.

## fdctl Base Links

The rows below describe the normal link contracts created by `src/app/fdctl/topology.c` and its network helper. `pohh` represents the Agave address space for externally published Agave messages; the runtime producer is identified where it differs from PoH hashing. GUI output uses the `guih` tile. BAM-only links remain in the separate BAM overlay table.

Network provider RX burst is `0UL` for XDP, `tile->mlx5.batch_size` for mlx5, and `64UL` for socket networking. When mlx5 adds its loopback socket helper, it appends `net_quic` and `net_shred` link instances in the `sock` workspace with depth `4096UL` and burst `64UL`; they inherit the selected mlx5 tile's consumers and input modes.

| Link | Producer | Consumers | Reliability / polling | Depth | MTU / burst | Workspace / gate | Payload and semantics |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `net_quic[k]` | network provider[k] | all quic tiles | Unreliable, polled | `config->net.ingress_buffer_size` | `FD_NET_MTU`; provider RX burst | `net_umem`; all fdctl modes | TPU UDP/QUIC network ingress. |
| `net_shred[k]` | network provider[k] | all shred tiles | Unreliable, polled | `config->net.ingress_buffer_size` | `FD_NET_MTU`; provider RX burst | `net_umem`; all fdctl modes | Shred network ingress. |
| `quic_net[i]` | quic[i] | all network provider tiles | Unreliable, polled | `config->net.ingress_buffer_size` | `FD_NET_MTU`, burst `1` | `net_quic`; all fdctl modes | Outbound QUIC datagrams. |
| `shred_net[i]` | shred[i] | all network provider tiles | Unreliable, polled | `32768UL` | `FD_NET_MTU`, burst `1` | `net_shred`; all fdctl modes | Outbound shreds. |
| `net_netlnk[k]` | net[k] or mlx5[k] | netlnk | Unreliable, polled | `128UL` | `0UL`, burst `0` | `net_netlnk`; XDP or mlx5 | Network-to-netlink neighbor-resolution/control requests. |
| `iproute_out` | netlnk | all net or mlx5 tiles | Reliable, polled | `fd_ulong_pow2_up(4UL*(netlnk_max_routes+netlnk_max_peer_routes)+8UL)` | `sizeof(fd_iproute_msg_t)`, burst `1` | `iproute`; XDP or mlx5 | Route updates/dumps. The max-route arguments come from tiles.netlink configuration. |
| `quic_verify[i]` | quic[i] | all verify tiles | Unreliable, polled | `config->tiles.verify.receive_buffer_size` | `sizeof(fd_tpu_msg_t)`, burst `config->tiles.quic.txn_reassembly_count` | `quic_verify`; all fdctl modes | Reassembled TPU transaction messages; sequence-based round-robin at verify. |
| `verify_dedup[i]` | verify[i] | dedup | Reliable, polled | `config->tiles.verify.receive_buffer_size` | `FD_TPU_PARSED_MTU`, burst `1` | `verify_dedup`; all fdctl modes | Parsed/verified transactions and retained BAM rejection markers. |
| `gossip_dedup` | Agave via pohh | dedup | Reliable, polled | `2048UL` | `FD_TPU_RAW_MTU`, burst `1` | `gossip_dedup`; all fdctl modes | Gossip votes wrapped as fd_txn_m_t, with source_tpu=GOSSIP; dedup parses this path. |
| `dedup_resolh` | dedup | all resolh tiles | Reliable, polled | `65536UL` | `FD_TPU_PARSED_MTU`, burst `1` | `dedup_resolh`; all fdctl modes | Transactions entering address lookup and blockhash resolution. |
| `resolh_pack[i]` | resolh[i] | pack | Reliable, polled | `65536UL` | `FD_TPU_RESOLVED_MTU`, burst `1` | `resolh_pack`; all fdctl modes | Resolved transactions and retained BAM rejection markers. |
| `stake_out` | Agave via pohh | pohh, all shred tiles, plugin when GUI enabled | Reliable, polled | `128UL` | `FD_STAKE_OUT_MTU`, burst `1` | `stake_out`; all fdctl modes, plugin consumer requires tiles.gui.enabled | Leader schedule/stake data; plugin forwards it as a leader-schedule update. |
| `pack_bank[i]` | pack | bank[i], guih when GUI enabled | Reliable, polled | `256UL` | `USHORT_MAX`, burst `1` | `pack_bank`; all fdctl modes | Scheduled transactions and microblock metadata. |
| `pack_pohh` | pack | pohh, guih when GUI enabled | Reliable, polled | `65536UL` | `sizeof(fd_done_packing_t)`, burst `1` | `pack_pohh`; all fdctl modes | Packing completion for the leader slot. |
| `bank_pohh[i]` | bank[i] | pohh, guih when GUI enabled | Reliable, polled | `16384UL` | `USHORT_MAX`, burst `1` | `bank_pohh`; all fdctl modes | Execution output/microblocks; provisional committed BAM result attached when applicable. |
| `bank_pack[i]` | bank[i] | pack when tiles.pack.use_consumed_cus | Unreliable, polled | `16384UL` | `USHORT_MAX`, burst `3` | `bank_pack`; link/output always created; pack input requires tiles.pack.use_consumed_cus | CU rebates from execution. |
| `pohh_pack` | pohh | pack; guih when GUI enabled | pack: unreliable/polled; guih: reliable/polled | `128UL` | `sizeof(fd_became_leader_t)`, burst `1` | `bank_pohh`; all fdctl modes | Leader notification. Pack acknowledges with packing completion, limiting notifications in flight without a reliable credit cycle. |
| `pohh_shred` | pohh | all shred tiles | Reliable, polled | `16384UL` | `USHORT_MAX`, burst `2` | `pohh_shred`; all fdctl modes | PoH entries/ticks and leader/epoch controls for shredding; distinct from the same-named shared shred-version fseq. |
| `crds_shred` | Agave via pohh | all shred tiles | Reliable, polled | `128UL` | `8UL+108000UL*46UL`, burst `1` | `pohh_shred`; all fdctl modes | Cluster/contact data for shred destinations. |
| `replay_resol` | Agave via pohh | all resolh tiles and all shred tiles | resolh: reliable/polled; shred: unreliable/polled | `128UL` | `sizeof(fd_completed_bank_t)`, burst `1` | `bank_pohh`; all fdctl modes | Root-bank/completed-blockhash progress for resolver and shred paths. |
| `executed_txn` | Agave replay via pohh | dedup and pack | Reliable, polled | `16384UL` | `64UL`, burst `1` | `executed_txn`; all fdctl modes | Committed replay transaction signatures; this fdctl path exists independently of BAM and does not emit full-PoH BAM completed-unlanded tags. |
| `shred_store[i]` | shred[i] | store | Reliable, polled | `65536UL` | `4UL*FD_SHRED_STORE_MTU`, burst `4UL+config->tiles.shred.max_pending_shred_sets` | `shred_store`; all fdctl modes | Completed FEC-set shred batches for Agave storage. |
| `shred_sign[i]` | shred[i] | sign[0] | Unreliable, polled | `128UL` | `32UL`, burst `1` | `shred_sign`; all fdctl modes | FD_KEYGUARD_ROLE_LEADER signing request; asynchronous with bounded pending requests. |
| `sign_shred[i]` | sign[0] | shred[i] | Unreliable, polled | `128UL` | `64UL`, burst `1` | `sign_shred`; all fdctl modes | Asynchronous shred signature response, processed as an ordinary polled fragment. |
| `plugin_out` | plugin | guih | Reliable, polled | `128UL` | `fd_ulong_max(FD_STAKE_OUT_MTU,8UL+108000UL*(60UL+12UL*6UL))`, burst `1` | `plugin_out`; tiles.gui.enabled | Forwarded plugin messages; stake updates are tagged `FD_PLUGIN_MSG_LEADER_SCHEDULE`, while other message signals are retained. |
| `replay_plugi` | Agave via pohh | plugin | Reliable, polled | `128UL` | `4098*8UL`, burst `1` | `plugin_in`; tiles.gui.enabled | Replay stage/root/optimistic/completed/reset/genesis notifications. |
| `gossip_plugi` | Agave via pohh | plugin | Reliable, polled | `128UL` | `8UL+108000UL*(60UL+12UL*6UL)`, burst `1` | `plugin_in`; tiles.gui.enabled | Gossip/vote-account/balance updates. |
| `pohh_plugin` | pohh | plugin | Reliable, polled | `128UL` | `16UL`, burst `1` | `plugin_in`; tiles.gui.enabled | Leader slot start/end notifications. |
| `startp_plugi` | Agave via pohh | plugin | Reliable, polled | `128UL` | `56UL`, burst `1` | `plugin_in`; tiles.gui.enabled | Startup progress updates. |
| `votel_plugin` | Agave via pohh | plugin | Reliable, polled | `128UL` | `8UL`, burst `1` | `plugin_in`; tiles.gui.enabled | Optimistically confirmed slot notifications from vote listener. |
| `valcfg_plugi` | Agave via pohh | plugin | Reliable, polled | `128UL` | `608UL`, burst `1` | `plugin_in`; tiles.gui.enabled | Validator information/config updates. |
| `bundle_verif` | bundle | all verify tiles | Reliable, polled | `config->tiles.verify.receive_buffer_size` | `FD_TPU_PARSED_MTU`, burst `1` | `bundle_verif`; tiles.bundle.enabled | Block Engine packets/bundles. Unsignaled packets may round-robin; nonzero bundle signals remain on verify[0]. |
| `bundle_sign` | bundle | sign[0] | Unreliable, polled | `65536UL` | `9UL`, burst `1` | `bundle_sign`; tiles.bundle.enabled | FD_KEYGUARD_ROLE_BUNDLE authentication signing request. |
| `sign_bundle` | sign[0] | bundle | Unreliable, unpolled | `128UL` | `64UL`, burst `1` | `sign_bundle`; tiles.bundle.enabled | Synchronous bundle authentication response, read out of band. |
| `bundle_status` | bundle | guih | Reliable, polled | `65536UL` | `sizeof(fd_bundle_block_engine_update_t)`, burst `1` | `bundle_status`; tiles.bundle.enabled and tiles.gui.enabled | Block Engine status/config updates. |
| `pack_sign` | pack | sign[0] | Unreliable, polled | `65536UL` | `1232UL`, burst `1` | `pack_sign`; tiles.bundle.enabled or tiles.bam.enabled | FD_KEYGUARD_ROLE_BUNDLE_CRANK crank transaction signing request. |
| `sign_pack` | sign[0] | pack | Unreliable, unpolled | `128UL` | `64UL`, burst `1` | `sign_pack`; tiles.bundle.enabled or tiles.bam.enabled | Synchronous crank signature response, read out of band. |

## Shared Objects And Side Channels

| Object | Writer(s) | Reader(s) | Topology | Semantics |
| --- | --- | --- | --- | --- |
| `execle_busy.%lu` fseq | `execle[i]` | `pack` | Full Firedancer | Execution progress latch used by pack to unlock accounts after execution/commit finishes; the executor advances it before publishing the microblock. |
| `bank_busy` objects stored under property key `execle_busy.%lu` | `bank[i]` execution workers in Agave | `pack` | fdctl | Execution progress latch. Topology assigns read-write access to `pohh` as the representative of the Agave address space; bank execution advances the latch before microblock publication. |
| `pohh_shred` fseq | Agave boot via `fd_ext_shred_set_shred_version` | all `shred` tiles | fdctl | Shared shred-version value; topology read-write owner is `pohh`. Distinct from the same-named fragment link. |
| `rnonce_ss` | object initialization callback | `repair`/`rotor`, all `shred` tiles | Full Firedancer | Startup-generated shared nonce secret, copied into tile-local state; repair/rotor has a read-write topology mapping. |
| `bam_status` fseq | `bam`, optional `bundle` | `bam`, `pack`, optional `bundle` | Full Firedancer and fdctl BAM | `OVERRIDE_ACTIVE` stops scheduling ordinary non-vote transactions (pack still buffers them) and drops direct Block Engine work; simple votes remain eligible. `BUNDLE_PUBLISHING` lets bundle claim an in-progress drain so BAM does not activate ownership concurrently. |
| `bam_gen` fseq | `bam`, `pack` | `bam`, `pack` | Full Firedancer and fdctl BAM; `bam_status` workspace | BAM sets the request bit for an ownership generation; pack retires prior-generation work and clears the bit to acknowledge activation or release. |
| `bam_gossip` consumer fseq | `gossip` consumer progress | `bam` | Full Firedancer BAM | Contact-update consumption progress, used with `bam_gossip_signed` for the [contact handoff](firebam-coordination.md#ownership-and-health). |
| `bam_gossip_signed` fseq | `gossip` | `bam` | Full Firedancer BAM | Acknowledges that a signed ContactInfo from this instance advertising the requested endpoints/client ID is installed in local CRDS. This is not remote gossip-peer acknowledgement. |
| `bam_ctrl` | CLI commands and `bam` | CLI commands, `bam`, and `replay` (full) or `pohh` (fdctl) | Full Firedancer and fdctl BAM | Shared admin control block for `set-bam`/`get-bam`. CLI CASes request state; BAM applies and writes success/error/current fields. Replay/pohh reads the applied mode at reset boundaries for slot timing. |
| `bam_fee_cfg` | `bam` | `pack` | Full Firedancer and fdctl BAM | Shared fee configuration from scheduler/BAM to pack. |
| key switch objects | identity/voter control path and each owning tile's handoff state updates | owning tile | Per-tile identity/authorized-voter objects | Shared key-material handoff, not a fragment stream. Full Firedancer grants `admin` read-write access to every keyswitch object; each owning tile also has read-write access. |
| `net_umem` dcaches | selected network provider | network RX consumers | Any network topology | RX storage; XDP/mlx5 RX links point into provider-owned UMEM. TX dcaches use their own `net_*` workspaces; mlx5 loopback RX clones use the `sock` workspace. |
| `accdb_data` (property `accdb`), `banks`, `progcache`, `txncache`, `store` | object-specific replay, execution, account-storage, shred/recovery, and snapshot owners | configured tile users | Full Firedancer | Shared state structures; actual access permissions are assigned by `fd_topob_tile_uses` in the topology builder. They are not fragment streams. |
| `snapin_shmem`, `frame_ticket` fseq | all `snapin` tiles for shared ingest state; `snapdc` tiles for frame tickets | corresponding snapshot-loader users | Full Firedancer snapshot load | Shared parallel snapshot-ingest state and decompression frame allocation. |
| `backup`, `snapzp.fseq`, `snaprd_out` dcache | `snapmk`/`snapzp` for backup/compression state; `snaprd` for its output dcache | `snapmk`, `snapzp` | Full Firedancer snapshot production | Backup/compression objects. `snapzp` reads the `snaprd_out` dcache directly without consuming that link's mcache. |
| `accdb_epoch.rpc`, `accdb_epoch.resolv.%lu`, `accdb_epoch.snapzp.%lu`, `accdb_epoch.snapmk` fseqs | named account-database joiner | `accdb` | Full Firedancer, when the joiner exists | Joiner epoch announcements let account storage defer partition reclamation during reads. |

## Edge Semantics

Normal mcache/dcache links are FIFO per producer link. Reliable consumers provide flow-control pressure; unreliable consumers may overrun or miss fragments. Fanout is implemented by multiple consumers on one link. Round-robin is tile-specific logic, not a property of the link object.

`quic_verify` is intentionally unreliable. All verify tiles consume all QUIC links and the verify tile only processes fragments assigned to it by sequence-based round-robin. The topology comments explicitly allow overrun.

`bundle_verif` fans into all verify tiles. Bundle-origin packets with signal `0` round-robin; nonzero bundle signals are handled by verify tile 0 to avoid interleaving bundles. `bam_verif` is wired only to verify tile 0 in both topologies, and BAM input handling selects tile 0 regardless of signal. BAM transactions are tagged with `FD_TXN_M_TPU_SOURCE_BAM`; verify marks parse/signature failures `preprocess_failed`, dedup and resolv/resolh propagate the metadata, and pack emits the terminal result on `pack_bam_res`.

For BAM runtime semantics, see the coordination reference's
[feedback/control roles](firebam-coordination.md#bam-feedback-and-control-roles),
[ownership and health](firebam-coordination.md#ownership-and-health), and
[feedback lifetime](firebam-coordination.md#feedback-lifetime). These cover
leader snapshot coalescing, provisional commitment, contact and generation
handoffs, shred forwarding, and result retention/drop rules.

Keyguard response modes depend on the client. Full Firedancer gossip, shred, and repair/rotor process asynchronous responses as polled fragments; fdctl shred does likewise. BAM, bundle, pack, and the other synchronous clients read response mcaches out of band, so those response inputs remain unpolled by stem.

## Topology Differences

Full Firedancer now has BAM topology wiring behind `leader_enabled && config->tiles.bam.enabled`. The current differences from fdctl are:

- Full Firedancer publishes not-committed outcomes from `execle` and resolves provisional committed outcomes in `poh`/`motor`; fdctl uses `bank` and `pohh`. In both topologies, verify/resolver failures flow through pack and are published on `pack_bam_res`.
- Full Firedancer has `bam_gossip` so BAM can update the C gossip tile's contact-info state directly. fdctl has no `bam_gossip` link and instead uses the Agave admin RPC path.
- fdctl has optional `bam_plugi` output for plugin/GUI status. Full Firedancer does not currently wire `bam_plugi`.
- Full Firedancer consumes `replay_slot` for BAM leader-schedule hints. fdctl creates a BAM-local `replay_out` link from `pohh` with depth `128`.
