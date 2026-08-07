#include "topology.h"
#include "config.h"
#include "../shared/fd_config_private.h"
#include "../shared/fd_action.h"

char const * FD_APP_NAME    = "test_fdctl_topology_bam";
char const * FD_BINARY_NAME = "test_fdctl_topology_bam";

action_t * ACTIONS[] = { NULL };

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

STUB_TILE( net    );
STUB_TILE( netlnk );
STUB_TILE( sock   );
STUB_TILE( quic   );
STUB_TILE( bundle );
STUB_TILE( bam    );
STUB_TILE( verify );
STUB_TILE( dedup  );
STUB_TILE( pack   );
STUB_TILE( shred  );
STUB_TILE( sign   );
STUB_TILE( metric );
STUB_TILE( diag   );
STUB_TILE( guih   );
STUB_TILE( plugin );
STUB_TILE( resolh );
STUB_TILE( pohh   );
STUB_TILE( bank   );
STUB_TILE( store  );

fd_topo_run_tile_t * TILES[] = {
  &stub_tile_net,
  &stub_tile_netlnk,
  &stub_tile_sock,
  &stub_tile_quic,
  &stub_tile_bundle,
  &stub_tile_bam,
  &stub_tile_verify,
  &stub_tile_dedup,
  &stub_tile_pack,
  &stub_tile_shred,
  &stub_tile_sign,
  &stub_tile_metric,
  &stub_tile_diag,
  &stub_tile_guih,
  &stub_tile_plugin,
  &stub_tile_resolh,
  &stub_tile_pohh,
  &stub_tile_bank,
  &stub_tile_store,
  NULL,
};

#undef STUB_TILE

extern fd_topo_obj_callbacks_t fd_obj_cb_mcache;
extern fd_topo_obj_callbacks_t fd_obj_cb_dcache;
extern fd_topo_obj_callbacks_t fd_obj_cb_fseq;
extern fd_topo_obj_callbacks_t fd_obj_cb_metrics;
extern fd_topo_obj_callbacks_t fd_obj_cb_netdev_tbl;
extern fd_topo_obj_callbacks_t fd_obj_cb_neigh4_hmap;
extern fd_topo_obj_callbacks_t fd_obj_cb_keyswitch;
extern fd_topo_obj_callbacks_t fd_obj_cb_bam_ctrl;
extern fd_topo_obj_callbacks_t fd_obj_cb_bam_fee_cfg;
extern fd_topo_obj_callbacks_t fd_obj_cb_tile;

fd_topo_obj_callbacks_t * CALLBACKS[] = {
  &fd_obj_cb_mcache,
  &fd_obj_cb_dcache,
  &fd_obj_cb_fseq,
  &fd_obj_cb_metrics,
  &fd_obj_cb_netdev_tbl,
  &fd_obj_cb_neigh4_hmap,
  &fd_obj_cb_keyswitch,
  &fd_obj_cb_bam_ctrl,
  &fd_obj_cb_bam_fee_cfg,
  &fd_obj_cb_tile,
  NULL,
};

static void
test_topology( int bundle_enabled,
               int bam_enabled ) {
  static config_t config[1];
  fd_memset( config, 0, sizeof(config_t) );
  config->is_firedancer = 0;
  fd_config_load_buf( config, (char const *)fdctl_default_config,
                     fdctl_default_config_sz, "default.toml" );
  fd_config_validate( config );

  config->tiles.gui.enabled = 0;
  config->tiles.bundle.enabled = bundle_enabled;
  config->tiles.bam.enabled = bam_enabled;
  config->layout.shred_tile_count = 2U;
  config->tiles.shred.additional_shred_destinations_retransmit_cnt = 1UL;
  fd_cstr_ncpy( config->tiles.shred.additional_shred_destinations_retransmit[ 0 ], "localhost:12000",
                sizeof(config->tiles.shred.additional_shred_destinations_retransmit[ 0 ]) );
  config->tiles.shred.additional_shred_destinations_leader_cnt = 1UL;
  fd_cstr_ncpy( config->tiles.shred.additional_shred_destinations_leader[ 0 ], "198.51.100.42:13000",
                sizeof(config->tiles.shred.additional_shred_destinations_leader[ 0 ]) );

  fd_cstr_ncpy( config->net.interface, "lo", sizeof(config->net.interface) );
  fd_cstr_ncpy( config->layout.affinity, "f128", sizeof(config->layout.affinity) );
  config->frankendancer.layout.agave_affinity[ 0 ] = '\0';
  fd_cstr_ncpy( config->tiles.bundle.tip_distribution_program_addr,
                "11111111111111111111111111111111",
                sizeof(config->tiles.bundle.tip_distribution_program_addr) );
  fd_cstr_ncpy( config->tiles.bundle.tip_payment_program_addr,
                "11111111111111111111111111111111",
                sizeof(config->tiles.bundle.tip_payment_program_addr) );
  fd_cstr_ncpy( config->tiles.bundle.tip_distribution_authority,
                "11111111111111111111111111111111",
                sizeof(config->tiles.bundle.tip_distribution_authority) );

  fd_topo_initialize( config );

  /* Without the shared crank and keyswitch path, BAM-only mode stops refreshing
     builder metadata and can leave setIdentity waiting indefinitely. */
  int crank_enabled = bundle_enabled || bam_enabled;
  fd_topo_t const * topo = &config->topo;

  ulong pack_id = fd_topo_find_tile( topo, "pack", 0UL );
  FD_TEST( pack_id!=ULONG_MAX );
  fd_topo_tile_t const * pack = &topo->tiles[ pack_id ];

  FD_TEST( (pack->id_keyswitch_obj_id!=ULONG_MAX)==crank_enabled );
  FD_TEST( (fd_topo_find_link( topo, "pack_sign", 0UL )!=ULONG_MAX)==crank_enabled );
  FD_TEST( (fd_topo_find_link( topo, "sign_pack", 0UL )!=ULONG_MAX)==crank_enabled );
  FD_TEST( !!pack->pack.bundle.enabled==crank_enabled );

  ulong pohh_id = fd_topo_find_tile( topo, "pohh", 0UL );
  FD_TEST( pohh_id!=ULONG_MAX );
  FD_TEST( !!topo->tiles[ pohh_id ].pohh.bundle.enabled==crank_enabled );

  FD_TEST( (fd_topo_find_tile( topo, "bundle", 0UL )!=ULONG_MAX)==bundle_enabled );
  FD_TEST( (fd_topo_find_tile( topo, "bam",    0UL )!=ULONG_MAX)==bam_enabled );

  ulong shred0_id = fd_topo_find_tile( topo, "shred", 0UL );
  ulong shred1_id = fd_topo_find_tile( topo, "shred", 1UL );
  FD_TEST( shred0_id!=ULONG_MAX && shred1_id!=ULONG_MAX );
  fd_topo_tile_t const * shred0 = &topo->tiles[ shred0_id ];
  fd_topo_tile_t const * shred1 = &topo->tiles[ shred1_id ];

  FD_TEST( shred0->shred.adtl_dests_retransmit_cnt==1UL && shred1->shred.adtl_dests_retransmit_cnt==1UL );
  FD_TEST( shred0->shred.adtl_dests_leader_cnt==1UL && shred1->shred.adtl_dests_leader_cnt==1UL );
  FD_TEST( fd_ip4_addr_is_loopback( shred0->shred.adtl_dests_retransmit[ 0 ].ip ) );
  FD_TEST( shred0->shred.adtl_dests_retransmit[ 0 ].port==12000U );
  FD_TEST( shred0->shred.adtl_dests_leader[ 0 ].ip==FD_IP4_ADDR( 198, 51, 100, 42 ) );
  FD_TEST( shred0->shred.adtl_dests_leader[ 0 ].port==13000U );
  FD_TEST( !memcmp( shred0->shred.adtl_dests_retransmit, shred1->shred.adtl_dests_retransmit,
                    sizeof(shred0->shred.adtl_dests_retransmit) ) );
  FD_TEST( !memcmp( shred0->shred.adtl_dests_leader, shred1->shred.adtl_dests_leader,
                    sizeof(shred0->shred.adtl_dests_leader) ) );
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
