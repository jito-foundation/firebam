# Tile Communication Map

This map is based on the topology builders and tile init paths in:

- `src/disco/topo/fd_topo.h`
- `src/disco/topo/fd_topob.c`
- `src/disco/net/fd_net_tile_topo.c`
- `src/app/firedancer/topology.c`
- `src/app/fdctl/topology.c`
- BAM, bundle, verify, quic, pack, sign, shred, plugin tile sources under `src/disco/`

There are two production source topologies in this branch:

- Full Firedancer: `src/app/firedancer/topology.c`. Its leader execution path is `quic -> verify -> dedup -> resolv -> pack -> execle -> poh -> shred`. It has bundle support. It has `[tiles.bam]` config and the BAM run tile is registered, but this topology does not currently instantiate a `bam` tile or any BAM links.
- Frankendancer/fdctl: `src/app/fdctl/topology.c`. Its execution path is `quic -> verify -> dedup -> resolh -> pack -> bank -> pohh -> shred`. This is the topology that currently wires the BAM overlay.

## Topology Primitives

`fd_topob_link` creates a named link instance. The link kind id is the ordinal among links with the same name. Repeated links below are written as `[i]`; for example `quic_verify[i]` is kind id `i`.

Each normal link has an mcache and, when MTU is nonzero, a dcache. A link has one producer and may have many consumers. Reliability and polling are per consumer input, not per link. `FD_TOPOB_UNPOLLED` inputs are required to be unreliable and are used for synchronous side loops such as keyguard responses.

Network RX links created by `fd_topos_net_rx_link` are special:

- With XDP, their mcache is in `net_umem`, their dcache is the XDP UMEM object owned by the producer net tile, and their burst is `0`.
- With socket networking, they use a normal dcache in `net_umem` and burst `64`.

Unless noted otherwise, non-network producer-to-consumer links below are reliable and polled by `fd_stem`.

## Full Firedancer Link Matrix

This section covers `src/app/firedancer/topology.c`.

| Links | Producers | Consumers | Reliability / polling | Depth | MTU / burst | Workspace / gate | Payload and semantics |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `net_gossvf[k]`, `net_shred[k]`, `net_repair[k]`, `net_txsend[k]`, `net_quic[k]` | `net[k]` or `sock[k]` | `gossvf`, `shred`, `repair`, `txsend`, `quic`; optional `event`, `scap`, `gui` taps | Unreliable, polled | `config->net.ingress_buffer_size` | `FD_NET_MTU`; XDP burst `0`, socket burst `64` | `net_umem`; `net_quic` only when leader is enabled | Raw network datagrams. No reliable consumer is allowed, so overruns/drop are acceptable. |
| `gossip_net`, `shred_net[i]`, `repair_net`, `txsend_net`, `quic_net[i]` | `gossip`, `shred[i]`, `repair`, `txsend`, `quic[i]` | all `net`/`sock` tiles | Unreliable, polled | `32768` for gossip/shred, `config->net.ingress_buffer_size` for repair/txsend/quic | `FD_NET_MTU`, burst `1` | `net_gossip`, `net_shred`, `net_repair`, `net_txsend`, `net_quic` | Raw outbound datagrams. Network boundary, no backpressure contract. |
| `net_netlnk[k]` | `net[k]` | `netlnk` | Unreliable, polled | `128` | MTU `0`, burst `0` | XDP provider only, `net_netlnk` | Net tile control requests into netlink/config plane. |
| `genesi_out` | `genesi` | `ipecho`, `repair`, `replay`, optional `event`, `gui` | Reliable, polled | `1` | `FD_GENESIS_TILE_MTU`, burst `1` | `genesi_out` | Genesis/bootstrap fragment. |
| `ipecho_out` | `ipecho` | `gossvf`, `gossip`, `shred`, `replay`, `tower`, optional `event`, `gui` | Reliable, polled | `2` | MTU `0`, burst `1` | `ipecho_out` | Contact/address bootstrap notification. |
| `gossvf_gossip[i]` | `gossvf[i]` | `gossip` | Unreliable, polled | `config->net.ingress_buffer_size` | `sizeof(fd_gossip_message_t)+FD_GOSSIP_MESSAGE_MAX_CRDS+FD_NET_MTU`, burst `1` | `gossvf_gossip` | Gossip packets after network filtering. |
| `gossip_gossvf` | `gossip` | all `gossvf` | Reliable, polled | `65536*4` | `sizeof(fd_gossip_ping_update_t)`, burst `1` | `gossip_gossvf` | Gossip ping/update state for verifier tiles. |
| `gossip_out` | `gossip` | `gossvf`, `repair`, `replay`, `tower`, `txsend`, `shred`, `verify`, optional snapshot, `scap`, `rpc`, `gui` | Reliable, polled | `65536*4` | `sizeof(fd_gossip_update_message_t)`, burst `1` | `gossip_out` | Cluster/gossip updates fanned out to many consumers. |
| `quic_verify[i]` | `quic[i]` | all `verify` tiles | Unreliable, polled | `config->tiles.verify.receive_buffer_size` | `FD_TPU_REASM_MTU`, burst `config->tiles.quic.txn_reassembly_count` | `quic_verify`; leader enabled | Reassembled TPU packets. Verify tiles round-robin by sequence; overrun is allowed. |
| `verify_dedup[i]` | `verify[i]` | `dedup` | Reliable, polled | `config->tiles.verify.receive_buffer_size` | `FD_TPU_PARSED_MTU`, burst `1` | `verify_dedup`; leader enabled | `fd_txn_m_t` plus payload after parse/signature checks. |
| `dedup_resolv` | `dedup` | `resolv[*]`, `tower`, optional `event` | Reliable, polled | `65536` | `FD_TPU_PARSED_MTU`, burst `1` | `dedup_resolv`; leader enabled | Deduplicated parsed transactions. Shared by resolv and observers. |
| `resolv_pack[0]` | `resolv[0]` | `pack` | Reliable, polled | `65536` | `FD_TPU_RESOLVED_MTU`, burst `1` | `resolv_pack`; leader enabled | Resolved transaction/account metadata to scheduler. Current configs use one resolv tile; additional `resolv_pack[i]` links would need corresponding pack inputs. |
| `resolv_replay[i]` | `resolv[i]` | `replay` | Reliable, polled | `4096` | `sizeof(fd_resolv_slot_exchanged_t)`, burst `1` | `resolv_replay` | Slot exchange notifications. |
| `pack_execle` | `pack` | all `execle` tiles | Reliable, polled | `65536` | `USHORT_MAX`, burst `1` | `pack_execle`; leader enabled | Scheduled microblock/transaction work. Shared across executors. |
| `execle_pack[i]` | `execle[i]` | `pack` | Reliable, polled | `16384` | `USHORT_MAX`, burst `1` | `execle_pack`; `tiles.pack.use_consumed_cus` and leader enabled | Consumed-CU feedback to pack. |
| `pack_poh` | `pack` | `poh`, optional `gui` | Reliable, polled | `4096` | `sizeof(fd_done_packing_t)`, burst `1` | `pack_poh`; leader enabled | Pack completion/slot progress signal. |
| `execle_poh[i]` | `execle[i]` | `poh`, optional `gui` | Reliable, polled | `16384` | `USHORT_MAX`, burst `1` | `execle_poh`; leader enabled | Executed transaction results to PoH. |
| `poh_shred` | `poh` | all `shred` tiles | Reliable, polled | `16384` | `USHORT_MAX`, burst `1` | `poh_shred`; leader enabled | PoH entries and shreds to shred tile. |
| `poh_replay` | `poh` | `replay` | Reliable, polled | `4096` | `sizeof(fd_poh_leader_slot_ended_t)`, burst `1` | `poh_replay`; leader enabled | Leader slot end feedback. |
| `replay_out` | `replay` | `dedup`, `resolv`, `repair`, `shred`, `tower`, `txsend`, `gossip`, `poh`, optional `rpc`, `scap`, `gui` | Reliable, polled | `65536` | `sizeof(fd_replay_message_t)`, burst `1` | `replay_out` | Replay state fanout. |
| `replay_epoch` | `replay` | `gossvf`, `gossip`, `shred`, `tower`, `txsend`, optional `gui` | Reliable, polled | `128` | `FD_EPOCH_OUT_MTU`, burst `1` | `replay_epoch` | Epoch/stake updates. |
| `replay_execrp` | `replay` | all `execrp` tiles | Reliable, polled | `16384` | `sizeof(fd_execrp_task_msg_t)`, burst `1` | `replay_execrp` | Execution/replay tasks. |
| `execrp_replay[i]` | `execrp[i]` | `replay`, optional `gui` | Reliable, polled | `16384` | `sizeof(fd_execrp_task_done_msg_t)`, burst `1` | `execrp_replay` | Execution/replay task completions. |
| `shred_out[i]` | `shred[i]` | `repair`, `tower`, optional `event`, `scap`, `gui` | Reliable except optional event/scap network captures are unreliable | `65536` | `FD_SHRED_OUT_MTU`, burst `3` | `shred_out` | Locally produced shred messages. |
| `repair_out` | `repair` | `replay` | Reliable, polled | `65536` | `FD_SHRED_OUT_MTU`, burst `1` | `repair_out` | Repaired shreds/messages. `scap` taps `repair_net`, not this link. |
| `tower_out` | `tower` | `repair`, `replay`, `shred`, `gossip`, `txsend`, optional `gui` | Reliable for most consumers; shred consumes it unreliable/polled | `16384` | `sizeof(fd_tower_msg_t)`, burst `2` | `tower_out` | Tower confirmations and slot-done messages. |
| `txsend_out` | `txsend` | `replay`, `gossip`, `verify[0]` | Reliable, polled | `128` | `FD_TPU_RAW_MTU`, burst `1` | `txsend_out` | Locally submitted transactions and status to peers. |
| `bundle_verif` | `bundle` | all `verify` tiles | Reliable, polled | `config->tiles.verify.receive_buffer_size` | `FD_TPU_PARSED_MTU`, burst `1` | `bundle_verif`; `tiles.bundle.enabled` | Bundle-origin parsed TPU packets. Verify treats bundle/BAM fan-in specially to avoid interleaving when signals are nonzero. |
| `bundle_status` | `bundle` | `gui` | Reliable, polled | `128` in full Firedancer, `65536` in fdctl | `sizeof(fd_bundle_block_engine_update_t)`, burst `1` | `bundle_status`; bundle and GUI enabled | Block Engine status updates for GUI. |
| `feeder`, `arch_f2w` | `replay`, `arch_f` | `arch_f`, `arch_w` | Reliable, polled | `65536`, `128` | `4*FD_SHRED_STORE_MTU`, burst `4+max_pending_shred_sets` for feeder; burst `1` for arch_f2w | archiver enabled | Archiver feed and writer handoff. |
| `rpc_replay` | `rpc` | `replay` | Reliable, polled | `8` | MTU `0`, burst `1` | RPC enabled | RPC request/control notifications into replay. |
| `cap_repl`, `cap_execrp[i]` | `replay`, `execrp[i]` | `solcap` | Reliable, polled | `32` | `SOLCAP_WRITE_ACCOUNT_DATA_MTU`, burst `1` | solcap enabled | Account capture feed. |
| `event_sign`, `sign_event` | `event`, `sign` | `sign`, `event` | Request unreliable/polled; response unreliable/unpolled | `128` | request `32`, response `64`, burst `1` | telemetry enabled | Keyguard signature loop for telemetry/event identity. |

## Snapshot Links

Snapshot links are gated by `snapshots_enabled`. They are reliable and polled unless otherwise noted.

| Links | Producers | Consumers | Depth | MTU / burst | Gate / semantics |
| --- | --- | --- | --- | --- | --- |
| `snapct_ld`, `snapld_dc`, `snapdc_in` | `snapct`, `snapld`, `snapdc` | `snapld`, `snapdc`, `snapin` | `128`, `16384`, `16384` | `sizeof(fd_ssctrl_init_t)`, `USHORT_MAX`, `USHORT_MAX`; burst `1` | Snapshot load pipeline. |
| `snapin_manif` | `snapin` | `gossip`, `repair`, `replay` | `4` | `sizeof(fd_snapshot_manifest_t)`, burst `1` | Loaded manifest publication. |
| `snapct_repr` | `snapct` | currently permitted to have no consumers | `128` | MTU `0`, burst `1` | Repair hook planned but not wired. |
| `snapct_gui`, `snapin_gui` | `snapct`, `snapin` | `gui` | `128` | `sizeof(fd_snapct_update_t)`, `FD_GUI_CONFIG_PARSE_MAX_VALID_ACCT_SZ`; burst `1` | GUI snapshot updates. |
| `snapin_txn`, `snapin_wm`, `snapwm_wh`, `snapwh_wr` | `snapin`, `snapwm`, `snapwh` | `snapwm`, `snapwh`, `snapwr`, `snaplh` | `16`, `16`, `64`, `64` | see topology constants; burst `1` | Vinyl snapshot writer/hash flow. `snapwh_wr` shares `snapwm_wh` dcache intentionally. |
| `snapwm_ct`, `snapin_ct` | `snapwm` or `snapin` | `snapct` | `128` | MTU `0`, burst `1` | LTHash disabled completion path. |
| `snaplh_lv`, `snapwm_lv`, `snaplv_lh`, `snaplv_ct` | `snaplh`, `snapwm`, `snaplv` | `snaplv`, `snaplh`, `snapct` | `128`, `32768`, `262144`, `128` | hash/dedup constants in topology; burst `1` except `snaplv_lh` uses `FD_SNAPLV_STEM_BURST` | Vinyl LTHash enabled path. |
| `snapla_ls`, `snapin_ls`, `snapls_ct` | `snapla`, `snapin`, `snapls` | `snapls`, `snapct` | `128`, `256`, `128` | account/hash constants; burst `1` | Non-vinyl LTHash enabled path. |

## Keyguard / Sign Links

Sign links are synchronous request-response channels unless noted. The client publishes to a request link, then spins on the response link out of band. This is why most response inputs are unreliable and unpolled.

| Request -> response | Producer / consumer | Role | Request MTU | Response MTU | Reliability / polling | Gate |
| --- | --- | --- | --- | --- | --- | --- |
| `gossip_sign` -> `sign_gossip` | `gossip` <-> `sign[0]` | `FD_KEYGUARD_ROLE_GOSSIP` | `2048` | `64` | request unreliable/polled, response unreliable/unpolled | Always |
| `shred_sign[i]` -> `sign_shred[i]` | `shred[i]` <-> `sign[0]` | `FD_KEYGUARD_ROLE_LEADER` | `32` | `64` | request unreliable/polled, response unreliable/unpolled | Always |
| `repair_sign[i]` -> `sign_repair[i]` | `repair` <-> `sign[i+1]` | `FD_KEYGUARD_ROLE_REPAIR` | `FD_REPAIR_MAX_PREIMAGE_SZ` (`96`) | `64` | request reliable/polled, response unreliable/polled because repair signing is async | Full Firedancer has `sign_tile_cnt-1` repair signers |
| `txsend_sign` -> `sign_txsend` | `txsend` <-> `sign[0]` | `FD_KEYGUARD_ROLE_TXSEND` | `FD_TXN_MTU` | `128` | request reliable/polled, response unreliable/unpolled | Always |
| `bundle_sign` -> `sign_bundle` | `bundle` <-> `sign[0]` | `FD_KEYGUARD_ROLE_BUNDLE` | `9` | `64` | request unreliable/polled, response unreliable/unpolled | Bundle enabled |
| `pack_sign` -> `sign_pack` | `pack` <-> `sign[0]` | `FD_KEYGUARD_ROLE_BUNDLE_CRANK` | `1232` | `64` | request unreliable/polled, response unreliable/unpolled | Bundle+leader in full Firedancer; bundle or BAM in fdctl |
| `bam_sign` -> `sign_bam` | `bam` <-> `sign[0]` | `FD_KEYGUARD_ROLE_BAM` | `256` | `64` | request unreliable/polled, response unreliable/unpolled | fdctl BAM only |
| `event_sign` -> `sign_event` | `event` <-> `sign[0]` | `FD_KEYGUARD_ROLE_EVENT` | `32` | `64` | request unreliable/polled, response unreliable/unpolled | Telemetry enabled |

## Frankendancer / fdctl BAM Overlay

This section covers the BAM links currently wired in `src/app/fdctl/topology.c` when `config->tiles.bam.enabled` is true. These links are not currently wired in `src/app/firedancer/topology.c`.

| Link | Kind ids | Producer | Consumers | Reliability / polling | Depth | MTU / burst | Workspace / gate | Payload and semantics |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `bam_verif` | `0` | `bam` | all `verify` tiles | Reliable, polled | `config->tiles.verify.receive_buffer_size` | `FD_TPU_PARSED_MTU`, burst `1` | `bam_verif`; BAM enabled | BAM scheduler transactions as `fd_txn_m_t` plus payload with `source_tpu=FD_TXN_M_TPU_SOURCE_BAM`. Verify forwards successes to `verify_dedup`; parse/verify failures are reported to `bank_bam`. |
| `bam_sign` | `0` | `bam` | `sign[0]` | Unreliable, polled | `65536` | `256`, burst `1` | `bam_sign`; BAM enabled | BAM auth challenge signing request. |
| `sign_bam` | `0` | `sign[0]` | `bam` | Unreliable, unpolled | `128` | `64`, burst `1` | `sign_bam`; BAM enabled | BAM auth signature response, consumed by keyguard client spin loop. |
| `pack_bam_ldr` | `0` | `pack` | `bam` | Unreliable, polled | `FD_BAM_MAX_PENDING_RESULTS` | `sizeof(fd_bam_leader_state_t)`, burst `1` | `pack_bam_ldr`; BAM enabled | Leader state snapshots: slot, tick, CU budget, slot end time, and current-slot BAM work bit. BAM coalesces this as latest-value-wins; newer snapshots may supersede older ones while BAM is reconnecting or behind. |
| `pack_bam_res` | `0` | `pack` | `bam` | Reliable, polled | `FD_BAM_MAX_PENDING_RESULTS` | `sizeof(fd_bam_bundle_result_t)`, burst `1` | `pack_bam_res`; BAM enabled | Durable bundle scheduling/result feedback from pack. BAM enqueues into a FIFO retained across scheduler reconnects. |
| `bank_bam[i]` | `0..bank_tile_cnt-1` | `bank[i]` | `bam` | Reliable, polled | `FD_BAM_MAX_PENDING_RESULTS` | `sizeof(fd_bam_bundle_result_t)`, burst `1` | `bank_bam`; BAM enabled | Durable executed bundle results from bank tiles. BAM enqueues into the same FIFO as `pack_bam_res`. |
| `bank_bam[bank_tile_cnt+i]` | `bank_tile_cnt..bank_tile_cnt+verify_tile_cnt-1` | `verify[i]` | `bam` | Reliable, polled | `FD_BAM_MAX_PENDING_RESULTS` | `sizeof(fd_bam_bundle_result_t)`, burst `1` | `bank_bam`; BAM enabled | BAM parse/signature failure reports from verify. Same durable FIFO semantics. |
| `bam_shred` | `0` | `bam` | all `shred` tiles | Reliable, polled | `128` | `sizeof(fd_bam_shred_update_t)`, burst `1` | `bam_shred`; BAM enabled | BAM shred receiver list update. Published with `FD_BAM_STEM_SIG_SHRED_UPDATE`; shred validates size and signal before replacing BAM destinations. |
| `bam_plugi` | `0` | `bam` | `plugin` | Reliable, polled | `65536` | `sizeof(fd_plugin_msg_bam_update_t)`, burst `1` | `bam_plugi`; BAM and plugin/GUI enabled | BAM status/config metrics to plugin/GUI. |

The BAM tile also has an external gRPC/TLS scheduler boundary. Inbound scheduler batches are converted into `bam_verif` fragments. Outbound feedback uses the same scheduler stream: `pack_bam_ldr` is sent as leader state messages, while `pack_bam_res` and `bank_bam` feed result messages.

## fdctl Base Links

The fdctl topology uses the same QUIC/verify/dedup shape but with Agave bank/PoH tiles:

| Links | Producers | Consumers | Reliability / polling | Depth | MTU / burst | Gate / semantics |
| --- | --- | --- | --- | --- | --- | --- |
| `quic_net[i]`, `shred_net[i]`, `net_quic[k]`, `net_shred[k]` | `quic`, `shred`, `net/sock` | `net/sock`, `quic`, `shred` | Unreliable, polled | `config->net.ingress_buffer_size` or `32768` | `FD_NET_MTU`; network RX burst depends on provider | Network TX/RX boundary. |
| `quic_verify[i]`, `verify_dedup[i]`, `gossip_dedup`, `dedup_resolh`, `resolh_pack[0]` | `quic`, `verify`, `pohh`, `dedup`, `resolh` | `verify`, `dedup`, `resolh`, `pack` | `quic_verify` unreliable/polled; others reliable/polled | `receive_buffer_size`, `2048`, `65536` | `FD_TPU_REASM_MTU`, `FD_TPU_PARSED_MTU`, `FD_TPU_RAW_MTU`, `FD_TPU_RESOLVED_MTU` | TPU ingress path. Current configs use one resolh tile; additional `resolh_pack[i]` links would need corresponding pack inputs. |
| `pack_bank`, `pack_pohh`, `bank_pohh[i]`, `bank_pack[i]`, `pohh_pack` | `pack`, `bank`, `pohh` | `bank`, `pohh`, `pack` | reliable/polled except `pohh_pack` and optional `bank_pack` are unreliable/polled | `65536`, `16384`, `128` | `USHORT_MAX` or small structs | Agave bank/PoH scheduling path. |
| `stake_out`, `pohh_shred`, `crds_shred`, `replay_resol`, `executed_txn`, `shred_store[i]` | `pohh`, `shred[i]` | `pohh`, `shred`, `plugin`, `resolh`, `dedup`, `pack`, `store` | reliable/polled except `replay_resol` into shred is unreliable/polled | `128` to `65536` | topology constants | Frankendancer stake, replay, shred, and store side flow. |
| `plugin_out`, `replay_plugi`, `gossip_plugi`, `pohh_plugin`, `startp_plugi`, `votel_plugin`, `valcfg_plugi` | `plugin`, `pohh` | `plugin`, external plugin consumer | Reliable, polled | `128` | plugin message MTUs | Plugin/GUI enabled. |
| `bundle_verif`, `bundle_sign`, `sign_bundle`, `bundle_status`, `bundle_plugi` | `bundle`, `sign` | `verify`, `sign`, `bundle`, `plugin` | same as full Firedancer bundle links | see full table | see full table | Bundle and optional plugin/GUI enabled. |

## Shared Objects And Side Channels

| Object | Writer(s) | Reader(s) | Topology | Semantics |
| --- | --- | --- | --- | --- |
| `execle_busy.%lu` fseq | `execle[i]` | `pack` | Full Firedancer | Latest progress latch used by pack to unlock accounts after execle/PoH finish a microblock. |
| `bank_busy` objects stored under property key `execle_busy.%lu` | `pohh` | `pack` | fdctl | Same role for Frankendancer bank/PoH path. The property key name is currently reused as `execle_busy.%lu`. |
| `pohh_shred` fseq | `pohh` | all `shred` tiles | fdctl | Shred version/control latch from Agave boot path to shred. |
| `rnonce_ss` | `repair` | all `shred` tiles | Full Firedancer | Repair/shred shared nonce secret object. |
| `bam_status` fseq | `bam` | `quic`, optional `bundle`; mapped read-only into `verify` | fdctl BAM | Bit 0 is `FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE`; bit 1 is `FD_BAM_STATUS_FSEQ_CURRENT_SLOT_HAS_BAM_WORK`. QUIC and bundle suppress/drain TPU ingestion while override is active; current verify code does not query it. |
| `bam_ctrl` | CLI commands and `bam` | CLI commands and `bam` | fdctl BAM | Shared admin control block for `set-bam`/`get-bam`. CLI CASes request state; BAM applies and writes success/error/current fields. |
| `bam_fee_cfg` | `bam` | `pack` | fdctl BAM | Shared fee configuration from scheduler/BAM to pack. |
| key switch objects | local tile/keyguard path | signing clients and sign tile | Any tile with identity/vote signing | Shared key material handoff; not an mcache/dcache stream. |
| `net_umem` dcaches | `net`/`sock` | network consumers | Any network topology | Packet data storage for network RX/TX. XDP RX links point directly into per-net UMEM. |
| `funk`, `funk_locks`, `banks`, `banks_locks`, `acc_pool`, `progcache`, `txncache`, `store`, `fec_sets`, `accdb`/vinyl objects | replay/exec/recovery owners depending on object | replay, execle, store, snapshot, rpc, gui as configured | Full Firedancer | Shared state stores. These are persistent data structures, not event links; access mode is encoded with `fd_topob_tile_uses`. |
| `vinyl_admin` and shared `snapwm_wh` dcache | snapshot vinyl tiles | snapshot vinyl tiles | Full Firedancer, snapshots+vinyl | Snapshot side channel for zero-copy writer/hash handoff. |

## Edge Semantics

Normal mcache/dcache links are FIFO per producer link. Reliable consumers provide flow-control pressure; unreliable consumers may overrun or miss fragments. Fanout is implemented by multiple consumers on one link. Round-robin is tile-specific logic, not a property of the link object.

`quic_verify` is intentionally unreliable. All verify tiles consume all QUIC links and the verify tile only processes fragments assigned to it by sequence-based round-robin. The topology comments explicitly allow overrun.

`bundle_verif` and `bam_verif` fan into the same verify path as QUIC, but the verify tile treats bundle/BAM sources specially. Fragments with signal `0` can round-robin. Nonzero bundle/BAM signals are handled by verify tile 0 to avoid interleaving bundle streams. BAM transactions are tagged with `FD_TXN_M_TPU_SOURCE_BAM`; parse/signature failures publish `fd_bam_bundle_result_t` onto `bank_bam` if that output is wired.

`pack_bam_ldr` is latest-value-wins. Pack publishes `fd_bam_leader_state_t` only when the state changes. The internal link is intentionally unreliable so stale snapshots do not backpressure pack; BAM stages the newest unsent state and may drop older pending states across reconnects.

`pack_bam_res` and `bank_bam` are durable FIFO feedback channels. BAM copies each `fd_bam_bundle_result_t` into an internal ring of `FD_BAM_MAX_PENDING_RESULTS`; the ring survives scheduler stream reset until flushed.

`bam_status` is a shared fseq latch, not a normal fragment stream. BAM writes override/current-work bits during housekeeping. QUIC and bundle consult it before ingesting TPU/block-engine traffic; fdctl topology also maps it read-only into verify, but current verify code does not query it.

`bam_shred` carries `fd_bam_shred_update_t` with signal `FD_BAM_STEM_SIG_SHRED_UPDATE`. Shred validates the signal and size, then updates BAM shred destinations. When destinations exist, shred sends leader/retransmit shreds to BAM receivers as configured.

Keyguard request/response links are synchronous. The requesting tile publishes one request and then spins on the response mcache with the keyguard client. Response links are unpolled by stem, so they do not participate in normal callback ordering.

## Gaps For Full Firedancer BAM

The branch currently has BAM tile code, config parsing, object callbacks, and run-tile registration under the Firedancer app, but `src/app/firedancer/topology.c` does not create:

- `bam` tile workspace or tile instance
- `bam_verif`
- `bam_sign` / `sign_bam`
- `pack_bam_ldr`
- `pack_bam_res`
- `bank_bam` or a full-Firedancer equivalent from `execle`/verify
- `bam_shred`
- `bam_plugi`
- `bam_status`
- `bam_ctrl`
- `bam_fee_cfg`

The current BAM overlay should therefore be treated as an fdctl/Frankendancer implementation to port into the full Firedancer `execle`/`poh` path, not as already-present full Firedancer wiring.
