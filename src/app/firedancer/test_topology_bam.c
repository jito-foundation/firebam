#include "topology.h"
#include "config.h"
#include "../shared/fd_config_private.h"
#include "../shared/fd_action.h"
#include "../../util/pod/fd_pod_format.h"
#include "../../flamenco/accdb/fd_accdb_cache.h"

char const * FD_APP_NAME    = "test_firedancer_topology_bam";
char const * FD_BINARY_NAME = "test_firedancer_topology_bam";

action_t * ACTIONS[] = { NULL };

extern fd_topo_obj_callbacks_t fd_obj_cb_mcache;
extern fd_topo_obj_callbacks_t fd_obj_cb_dcache;
extern fd_topo_obj_callbacks_t fd_obj_cb_fseq;
extern fd_topo_obj_callbacks_t fd_obj_cb_metrics;
extern fd_topo_obj_callbacks_t fd_obj_cb_netdev_tbl;
extern fd_topo_obj_callbacks_t fd_obj_cb_neigh4_hmap;
extern fd_topo_obj_callbacks_t fd_obj_cb_keyswitch;
extern fd_topo_obj_callbacks_t fd_obj_cb_node_info;
extern fd_topo_obj_callbacks_t fd_obj_cb_leader_txn_timing;
extern fd_topo_obj_callbacks_t fd_obj_cb_bam_ctrl;
extern fd_topo_obj_callbacks_t fd_obj_cb_bam_fee_cfg;
extern fd_topo_obj_callbacks_t fd_obj_cb_tile;
extern fd_topo_obj_callbacks_t fd_obj_cb_store;
extern fd_topo_obj_callbacks_t fd_obj_cb_fec_sets;
extern fd_topo_obj_callbacks_t fd_obj_cb_txncache;
extern fd_topo_obj_callbacks_t fd_obj_cb_accdb;
extern fd_topo_obj_callbacks_t fd_obj_cb_backup;
extern fd_topo_obj_callbacks_t fd_obj_cb_banks;
extern fd_topo_obj_callbacks_t fd_obj_cb_progcache;
extern fd_topo_obj_callbacks_t fd_obj_cb_rnonce_ss;
extern fd_topo_obj_callbacks_t fd_obj_cb_adminctl;

fd_topo_obj_callbacks_t * CALLBACKS[] = {
  &fd_obj_cb_mcache,
  &fd_obj_cb_dcache,
  &fd_obj_cb_fseq,
  &fd_obj_cb_metrics,
  &fd_obj_cb_netdev_tbl,
  &fd_obj_cb_neigh4_hmap,
  &fd_obj_cb_keyswitch,
  &fd_obj_cb_node_info,
  &fd_obj_cb_leader_txn_timing,
  &fd_obj_cb_bam_ctrl,
  &fd_obj_cb_bam_fee_cfg,
  &fd_obj_cb_tile,
  &fd_obj_cb_store,
  &fd_obj_cb_fec_sets,
  &fd_obj_cb_txncache,
  &fd_obj_cb_accdb,
  &fd_obj_cb_backup,
  &fd_obj_cb_banks,
  &fd_obj_cb_progcache,
  &fd_obj_cb_rnonce_ss,
  &fd_obj_cb_adminctl,
  NULL,
};

static ulong
stub_tile_scratch_align( void ) {
  return 1UL;
}

static ulong
stub_tile_scratch_footprint( fd_topo_tile_t const * tile ) {
  (void)tile;
  return 1UL;
}

#define STUB_TILE( tile_name )                             \
  static fd_topo_run_tile_t stub_tile_##tile_name = {     \
    .name              = #tile_name,                      \
    .scratch_align     = stub_tile_scratch_align,         \
    .scratch_footprint = stub_tile_scratch_footprint,     \
  }

STUB_TILE( net     );
STUB_TILE( netlnk  );
STUB_TILE( sock    );
STUB_TILE( quic    );
STUB_TILE( verify  );
STUB_TILE( dedup   );
STUB_TILE( resolv  );
STUB_TILE( pack    );
STUB_TILE( execle  );
STUB_TILE( shred   );
STUB_TILE( sign    );
STUB_TILE( metric  );
STUB_TILE( event   );
STUB_TILE( diag    );
STUB_TILE( gui     );
STUB_TILE( rpc     );
STUB_TILE( bundle  );
STUB_TILE( bam     );
STUB_TILE( gossvf  );
STUB_TILE( gossip  );
STUB_TILE( repair  );
STUB_TILE( rserve  );
STUB_TILE( replay  );
STUB_TILE( execrp  );
STUB_TILE( poh     );
STUB_TILE( txsend  );
STUB_TILE( tower   );
STUB_TILE( accdb   );
STUB_TILE( snapct  );
STUB_TILE( snapld  );
STUB_TILE( snapdc  );
STUB_TILE( snapin  );
STUB_TILE( snapwr  );
STUB_TILE( genesi  );
STUB_TILE( ipecho  );
STUB_TILE( admin   );
STUB_TILE( solcap  );
STUB_TILE( snapmk  );
STUB_TILE( snapzp  );
STUB_TILE( snaprd  );
STUB_TILE( snapsv  );

fd_topo_run_tile_t * TILES[] = {
  &stub_tile_net,
  &stub_tile_netlnk,
  &stub_tile_sock,
  &stub_tile_quic,
  &stub_tile_verify,
  &stub_tile_dedup,
  &stub_tile_resolv,
  &stub_tile_pack,
  &stub_tile_execle,
  &stub_tile_shred,
  &stub_tile_sign,
  &stub_tile_metric,
  &stub_tile_event,
  &stub_tile_diag,
  &stub_tile_gui,
  &stub_tile_rpc,
  &stub_tile_bundle,
  &stub_tile_bam,
  &stub_tile_gossvf,
  &stub_tile_gossip,
  &stub_tile_repair,
  &stub_tile_rserve,
  &stub_tile_replay,
  &stub_tile_execrp,
  &stub_tile_poh,
  &stub_tile_txsend,
  &stub_tile_tower,
  &stub_tile_accdb,
  &stub_tile_snapct,
  &stub_tile_snapld,
  &stub_tile_snapdc,
  &stub_tile_snapin,
  &stub_tile_snapwr,
  &stub_tile_genesi,
  &stub_tile_ipecho,
  &stub_tile_admin,
  &stub_tile_solcap,
  &stub_tile_snapmk,
  &stub_tile_snapzp,
  &stub_tile_snaprd,
  &stub_tile_snapsv,
  NULL,
};

#undef STUB_TILE

/* Exercise every BAM/bundle mode: missing shared crank/sign wiring breaks
   BAM-only config and identity updates, while an unwanted source tile leaves
   a disabled ingress path active. */
static void
test_topology( int bundle_enabled,
               int bam_enabled ) {
  static config_t config[1];
  fd_memset( config, 0, sizeof(config_t) );
  config->is_firedancer = 1;
  fd_config_load_buf( config, (char const *)firedancer_default_config,
                     firedancer_default_config_sz, "default.toml" );
  fd_config_validate( config );

  config->telemetry = 0;
  config->tiles.gui.enabled = 0;
  config->tiles.bundle.enabled = bundle_enabled;
  config->tiles.bam.enabled = bam_enabled;

  fd_cstr_ncpy( config->net.interface, "lo", sizeof(config->net.interface) );
  fd_cstr_ncpy( config->layout.affinity, "f128", sizeof(config->layout.affinity) );
  fd_cstr_ncpy( config->tiles.bundle.tip_distribution_program_addr,
                "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7",
                sizeof(config->tiles.bundle.tip_distribution_program_addr) );
  fd_cstr_ncpy( config->tiles.bundle.tip_payment_program_addr,
                "T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt",
                sizeof(config->tiles.bundle.tip_payment_program_addr) );
  fd_cstr_ncpy( config->tiles.bundle.tip_distribution_authority,
                "11111111111111111111111111111111",
                sizeof(config->tiles.bundle.tip_distribution_authority) );

  fd_topo_initialize( config );

  int crank_enabled = bundle_enabled || bam_enabled;
  fd_topo_t const * topo = &config->topo;

  ulong pack_id = fd_topo_find_tile( topo, "pack", 0UL );
  FD_TEST( pack_id!=ULONG_MAX );
  fd_topo_tile_t const * pack = &topo->tiles[ pack_id ];

  FD_TEST( (pack->id_keyswitch_obj_id!=ULONG_MAX)==crank_enabled );
  FD_TEST( (fd_topo_find_link( topo, "pack_sign", 0UL )!=ULONG_MAX)==crank_enabled );
  FD_TEST( (fd_topo_find_link( topo, "sign_pack", 0UL )!=ULONG_MAX)==crank_enabled );
  FD_TEST( !!pack->pack.bundle.enabled==crank_enabled );

  ulong sign_id = fd_topo_find_tile( topo, "sign", 0UL );
  FD_TEST( sign_id!=ULONG_MAX );
  fd_topo_tile_t const * sign = &topo->tiles[ sign_id ];
  uchar disabled_program[ 32 ];
  fd_memset( disabled_program, 0xFF, sizeof(disabled_program) );
  FD_TEST( fd_memeq( sign->sign.bundle.tip_distribution_program_addr,
                     crank_enabled ? pack->pack.bundle.tip_distribution_program_addr : disabled_program, 32UL ) );
  FD_TEST( fd_memeq( sign->sign.bundle.tip_payment_program_addr,
                     crank_enabled ? pack->pack.bundle.tip_payment_program_addr : disabled_program, 32UL ) );

  ulong replay_id = fd_topo_find_tile( topo, "replay", 0UL );
  FD_TEST( replay_id!=ULONG_MAX );
  FD_TEST( !!topo->tiles[ replay_id ].replay.bundle.enabled==crank_enabled );
  FD_TEST( (fd_topo_find_tile_obj( topo, &topo->tiles[ replay_id ], "bam_ctrl" )!=NULL)==bam_enabled );

  FD_TEST( (fd_topo_find_tile( topo, "bundle", 0UL )!=ULONG_MAX)==bundle_enabled );
  FD_TEST( (fd_topo_find_tile( topo, "bam",    0UL )!=ULONG_MAX)==bam_enabled );

  /* Omitting executed_txn outside BAM leaves landed transactions in pack and
     dedup, allowing stale copies to be scheduled again. */
  FD_TEST( fd_topo_find_link( topo, "executed_txn", 0UL )!=ULONG_MAX );

  fd_topo_obj_t const * accdb = fd_topo_find_obj( topo, "accdb", NULL, ULONG_MAX );
  FD_TEST( accdb );
  FD_TEST( fd_pod_queryf_ulong( topo->props, 0UL, "obj.%lu.cache_min_reserved", accdb->id )==
           fd_accdb_cache_min_reserved( crank_enabled ) );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  test_topology( 0, 0 );
  test_topology( 1, 0 );
  test_topology( 0, 1 );
  test_topology( 1, 1 );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
