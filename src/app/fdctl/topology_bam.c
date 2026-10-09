/* FireBAM part of the fdctl (Frankendancer) topology.  Included once
   by topology.c.  fd_topo_initialize calls each topo_bam_* function
   where its elements used to be added inline, so workspace, link,
   tile and object ids, in-link order and CPU assignment are unchanged. */

#include "../../discof/replay/fd_replay_tile.h"
#include "../../discoh/plugin/fd_plugin.h"
#include "../../disco/bam/fd_bam_types.h"
#include "../../disco/bam/fd_bam_ctrl.h"

#define BAM_DUMP_MODE( config )                                                     \
  ( (config)->development.bam.dump_bam_txns                                         \
    ? FD_BAM_DEBUG_DUMP_MODE_ALL                                                    \
    : ( (config)->development.bam.dump_bam_slot_first_txn                           \
        ? FD_BAM_DEBUG_DUMP_MODE_SLOT_FIRST                                         \
        : FD_BAM_DEBUG_DUMP_MODE_OFF ) )

#define FOR(cnt) for( ulong i=0UL; i<cnt; i++ )

/* Without the Block Engine, pack still needs a signer for the tip
   crank BAM uses. */
static void
topo_bam_tip_crank_links( fd_topo_t * topo ) {
  fd_topob_wksp( topo, "pack_sign"    );
  fd_topob_wksp( topo, "sign_pack"    );
  /**/                 fd_topob_link( topo, "pack_sign",    "pack_sign",    65536UL,                                  1232UL,                    1UL );
  /**/                 fd_topob_link( topo, "sign_pack",    "sign_pack",    128UL,                                    64UL,                      1UL );
  /**/                 fd_topob_tile_in(  topo, "sign",   0UL,           "metric_in", "pack_sign",      0UL,        FD_TOPOB_UNRELIABLE, FD_TOPOB_POLLED   );
  /**/                 fd_topob_tile_out( topo, "pack",   0UL,                        "pack_sign",      0UL                                                );
  /**/                 fd_topob_tile_in(  topo, "pack",   0UL,           "metric_in", "sign_pack",      0UL,        FD_TOPOB_UNRELIABLE, FD_TOPOB_UNPOLLED );
  /**/                 fd_topob_tile_out( topo, "sign",   0UL,                        "sign_pack",      0UL                                                );
}

static void
topo_bam_tiles( fd_topo_t *   topo,
                ulong const * tile_to_cpu,
                ulong         bank_tile_cnt,
                ulong         shred_tile_cnt,
                int           plugins_enabled ) {
  fd_topob_wksp( topo, "bam"         );
  fd_topob_wksp( topo, "bam_verif"   );
  fd_topob_wksp( topo, "bam_sign"    );
  fd_topob_wksp( topo, "sign_bam"    );
  fd_topob_wksp( topo, "pack_bam_ldr" );
  fd_topob_wksp( topo, "pack_bam_res" );
  fd_topob_wksp( topo, "bank_bam"    );
  fd_topob_wksp( topo, "poh_bam"     );
  fd_topob_wksp( topo, "bam_shred"   );
  fd_topob_wksp( topo, "replay_out"   );
  fd_topob_wksp( topo, "bam_status"  );
  fd_topob_wksp( topo, "bam_ctrl"    );
  fd_topob_wksp( topo, "bam_fee_cfg" );

  /**/                 fd_topob_link( topo, "bam_verif",  "bam_verif",  FD_BAM_VERIFY_OUT_DEPTH,                     FD_TPU_PARSED_MTU,          FD_BAM_STEM_BURST );
  /**/                 fd_topob_link( topo, "bam_sign",   "bam_sign",   128UL,                                    256UL,                       1UL );
  /**/                 fd_topob_link( topo, "sign_bam",   "sign_bam",   128UL,                                    64UL,                        1UL );
  /* Keep pack->bam leader snapshots and result feedback on separate
     internal links because BAM coalesces leader state but durably
     queues results across reconnects. The external scheduler stream
     is unchanged; this only removes the internal size-based mux. */
  /**/                 fd_topob_link( topo, "pack_bam_ldr", "pack_bam_ldr", FD_BAM_MAX_PENDING_RESULTS, sizeof(fd_bam_leader_state_t),  1UL );
  /**/                 fd_topob_link( topo, "pack_bam_res", "pack_bam_res", FD_BAM_RESULT_LINK_DEPTH,   sizeof(fd_bam_bundle_result_t), 1UL );
  FOR(bank_tile_cnt)   fd_topob_link( topo, "bank_bam",   "bank_bam",   FD_BAM_RESULT_LINK_DEPTH,                 sizeof(fd_bam_bundle_result_t), 1UL );
  /**/                 fd_topob_link( topo, "poh_bam",    "poh_bam",    FD_BAM_RESULT_LINK_DEPTH,                 sizeof(fd_bam_bundle_result_t), 1UL );
  /**/                 fd_topob_link( topo, "bam_shred",  "bam_shred",  128UL,                                    sizeof(fd_bam_shred_update_t), 1UL );
  /**/                 fd_topob_link( topo, "replay_out", "replay_out", 128UL,                                    sizeof(fd_replay_message_t),   1UL );

  /**/                 fd_topob_tile( topo, "bam",     "bam",     "metric_in",  tile_to_cpu[ topo->tile_cnt ], 0,        1,                 0,                 topo->sleep_obj_id!=ULONG_MAX );

  /**/                 fd_topob_tile_out( topo, "bam",    0UL,                        "bam_verif",    0UL                                                );
  /**/                 fd_topob_tile_in(  topo, "verify", 0UL,          "metric_in", "bam_verif",    0UL,          FD_TOPOB_RELIABLE,   FD_TOPOB_POLLED   );

  /**/                 fd_topob_tile_in(  topo, "sign",   0UL,           "metric_in", "bam_sign",     0UL,          FD_TOPOB_UNRELIABLE, FD_TOPOB_POLLED   );
  /**/                 fd_topob_tile_out( topo, "bam",    0UL,                        "bam_sign",     0UL                                                );

  /**/                 fd_topob_tile_out( topo, "pack",   0UL,                        "pack_bam_ldr", 0UL                                                );
  /**/                 fd_topob_tile_in(  topo, "bam",    0UL,           "metric_in", "pack_bam_ldr", 0UL,            FD_TOPOB_UNRELIABLE, FD_TOPOB_POLLED   );
  /**/                 fd_topob_tile_out( topo, "pack",   0UL,                        "pack_bam_res", 0UL                                                );
  /**/                 fd_topob_tile_in(  topo, "bam",    0UL,           "metric_in", "pack_bam_res", 0UL,            FD_TOPOB_UNRELIABLE, FD_TOPOB_POLLED   );
  FOR(bank_tile_cnt)   fd_topob_tile_out( topo, "bank",   i,                          "bank_bam",     i                                                  );
  FOR(bank_tile_cnt)   fd_topob_tile_in(  topo, "bam",    0UL,           "metric_in", "bank_bam",     i,            FD_TOPOB_UNRELIABLE, FD_TOPOB_POLLED   );
  /**/                 fd_topob_tile_out( topo, "pohh",   0UL,                        "poh_bam",      0UL                                                );
  /**/                 fd_topob_tile_in(  topo, "bam",    0UL,           "metric_in", "poh_bam",      0UL,          FD_TOPOB_UNRELIABLE, FD_TOPOB_POLLED   );
  /**/                 fd_topob_tile_out( topo, "bam",    0UL,                        "bam_shred",    0UL                                                );
  FOR(shred_tile_cnt)  fd_topob_tile_in(  topo, "shred",  i,             "metric_in", "bam_shred",    0UL,          FD_TOPOB_RELIABLE,   FD_TOPOB_POLLED   );
  /**/                 fd_topob_tile_out( topo, "pohh",   0UL,                        "replay_out",   0UL                                                );
  /**/                 fd_topob_tile_in(  topo, "bam",    0UL,           "metric_in", "replay_out",   0UL,          FD_TOPOB_UNRELIABLE, FD_TOPOB_POLLED   );

  /* sign_bam is read out of band, so keep it after BAM's polled feedback inputs. */
  /**/                 fd_topob_tile_in(  topo, "bam",    0UL,           "metric_in", "sign_bam",     0UL,          FD_TOPOB_UNRELIABLE, FD_TOPOB_UNPOLLED );
  /**/                 fd_topob_tile_out( topo, "sign",   0UL,                        "sign_bam",     0UL                                                );

  if( FD_LIKELY( plugins_enabled ) ) {
    fd_topob_wksp( topo, "bam_plugi" );
    /* bam_plugi must be kind of deep, to prevent exhausting shared
       flow control credits when publishing many packets at once. */
    fd_topob_link( topo, "bam_plugi", "bam_plugi", 65536UL, sizeof(fd_plugin_msg_bam_update_t), 1UL );
    fd_topob_tile_in(  topo, "plugin", 0UL, "metric_in", "bam_plugi", 0UL, FD_TOPOB_RELIABLE,   FD_TOPOB_POLLED );
    fd_topob_tile_out( topo, "bam", 0UL, "bam_plugi", 0UL );
  }
}

static void
topo_bam_objs( fd_topo_t *      topo,
               config_t const * config ) {
  fd_topo_obj_t * bam_status_obj = fd_topob_obj( topo, "fseq", "bam_status" );
  fd_topo_obj_t * bam_gen_obj    = fd_topob_obj( topo, "fseq", "bam_status" );
  fd_topo_tile_t * bam_tile      = &topo->tiles[ fd_topo_find_tile( topo, "bam", 0UL ) ];
  fd_topo_tile_t * pack_tile     = &topo->tiles[ fd_topo_find_tile( topo, "pack", 0UL ) ];
  fd_topob_tile_uses( topo, bam_tile, bam_status_obj, FD_SHMEM_JOIN_MODE_READ_WRITE );
  fd_topob_tile_uses( topo, pack_tile, bam_status_obj, FD_SHMEM_JOIN_MODE_READ_ONLY );
  fd_topob_tile_uses( topo, bam_tile, bam_gen_obj, FD_SHMEM_JOIN_MODE_READ_WRITE );
  fd_topob_tile_uses( topo, pack_tile, bam_gen_obj, FD_SHMEM_JOIN_MODE_READ_WRITE );
  if( FD_LIKELY( config->tiles.bundle.enabled ) ) {
    fd_topo_tile_t * bundle_tile = &topo->tiles[ fd_topo_find_tile( topo, "bundle", 0UL ) ];
    fd_topob_tile_uses( topo, bundle_tile, bam_status_obj, FD_SHMEM_JOIN_MODE_READ_WRITE );
  }
  FD_TEST( fd_pod_insertf_ulong( topo->props, bam_status_obj->id, "bam_status" ) );
  FD_TEST( fd_pod_insertf_ulong( topo->props, bam_gen_obj->id, "bam_gen" ) );

  fd_topo_obj_t * bam_ctrl_obj = fd_topob_obj( topo, "bam_ctrl", "bam_ctrl" );
  fd_topob_tile_uses( topo, bam_tile, bam_ctrl_obj, FD_SHMEM_JOIN_MODE_READ_WRITE );
  fd_topob_tile_uses( topo, &topo->tiles[ fd_topo_find_tile( topo, "pohh", 0UL ) ], bam_ctrl_obj, FD_SHMEM_JOIN_MODE_READ_ONLY );
  FD_TEST( fd_pod_insertf_ulong( topo->props, bam_ctrl_obj->id, "bam_ctrl" ) );

  fd_topo_obj_t * bam_fee_cfg_obj = fd_topob_obj( topo, "bam_fee_cfg", "bam_fee_cfg" );
  fd_topob_tile_uses( topo, bam_tile, bam_fee_cfg_obj, FD_SHMEM_JOIN_MODE_READ_WRITE );
  fd_topob_tile_uses( topo, pack_tile, bam_fee_cfg_obj, FD_SHMEM_JOIN_MODE_READ_ONLY );
  FD_TEST( fd_pod_insertf_ulong( topo->props, bam_fee_cfg_obj->id, "bam_fee_cfg" ) );

  /* The BAM tile connects to the Agave admin RPC before sandbox entry. */
  fd_pod_insert_int( topo->props, "sandbox", config->development.sandbox ? 1 : 0 );
}

#undef FOR

static void
topo_bam_configure_tile( fd_topo_tile_t *    tile,
                         fd_config_t const * config ) {
  uint configured_default_tpu_addr = FD_UNLIKELY( !config->gossip.entrypoints_cnt )
    ? FD_IP4_ADDR( 127, 0, 0, 1 )
    : 0U;
  if( FD_LIKELY( config->net.bind_address[ 0 ] ) ) {
    uint bind_addr = config->net.bind_address_parsed;
    if( FD_LIKELY( bind_addr &&
                   ( bind_addr<IP4_LOOPBACK_START_NET || bind_addr>IP4_LOOPBACK_END_NET ) ) )
      configured_default_tpu_addr = bind_addr;
  }

  fd_cstr_ncpy( tile->bam.url, config->tiles.bam.url, sizeof(tile->bam.url) );
  tile->bam.url_len = strnlen( tile->bam.url, sizeof(tile->bam.url)-1UL );
  fd_cstr_ncpy( tile->bam.sni, config->tiles.bam.tls_domain_name, sizeof(tile->bam.sni) );
  tile->bam.sni_len = strnlen( tile->bam.sni, sizeof(tile->bam.sni)-1UL );
  FD_TEST( fd_cstr_printf_check( tile->bam.admin_rpc_path,
                                 sizeof(tile->bam.admin_rpc_path),
                                 NULL,
                                 "%s/admin.rpc",
                                 config->frankendancer.paths.ledger ) );
  tile->bam.configured_default_tpu = (fd_ip4_port_t){
    .addr = configured_default_tpu_addr,
    .port = fd_ushort_bswap( config->tiles.quic.regular_transaction_listen_port )
  };
  fd_cstr_ncpy( tile->bam.identity_key_path, config->paths.identity_key, sizeof(tile->bam.identity_key_path) );
  fd_cstr_ncpy( tile->bam.key_log_path, config->development.bam.ssl_key_log_file, sizeof(tile->bam.key_log_path) );
  tile->bam.buf_sz = config->development.bam.buffer_size_kib<<10;
  tile->bam.out_depth = FD_BAM_VERIFY_OUT_DEPTH;
  tile->bam.keepalive_interval_nanos = config->tiles.bam.keepalive_interval_millis * (ulong)1e6;
  tile->bam.tls_cert_verify = !!config->tiles.bam.tls_cert_verify;
  tile->bam.enabled = !!config->tiles.bam.enabled;
  tile->bam.dump_bam_mode = BAM_DUMP_MODE( config );
}
