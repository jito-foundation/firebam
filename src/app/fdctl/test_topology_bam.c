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
               int bam_enabled,
               int gui_enabled ) {
  static config_t config[1];
  fd_memset( config, 0, sizeof(config_t) );
  config->is_firedancer = 0;
  fd_config_load_buf( config, (char const *)fdctl_default_config,
                     fdctl_default_config_sz, "default.toml" );
  fd_config_validate( config );

  config->tiles.gui.enabled = gui_enabled;
  config->tiles.bundle.enabled = bundle_enabled;
  config->tiles.bam.enabled = bam_enabled;

  fd_cstr_ncpy( config->net.interface, "lo", sizeof(config->net.interface) );
  fd_cstr_ncpy( config->layout.affinity, "f128", sizeof(config->layout.affinity) );
  config->frankendancer.layout.agave_affinity[ 0 ] = '\0';
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

  ulong sign_id = fd_topo_find_tile( topo, "sign", 0UL );
  FD_TEST( sign_id!=ULONG_MAX );
  fd_topo_tile_t const * sign = &topo->tiles[ sign_id ];
  uchar disabled_program[ 32 ];
  fd_memset( disabled_program, 0xFF, sizeof(disabled_program) );
  FD_TEST( fd_memeq( sign->sign.bundle.tip_distribution_program_addr,
                     crank_enabled ? pack->pack.bundle.tip_distribution_program_addr : disabled_program, 32UL ) );
  FD_TEST( fd_memeq( sign->sign.bundle.tip_payment_program_addr,
                     crank_enabled ? pack->pack.bundle.tip_payment_program_addr : disabled_program, 32UL ) );

  ulong pohh_id = fd_topo_find_tile( topo, "pohh", 0UL );
  FD_TEST( pohh_id!=ULONG_MAX );
  FD_TEST( !!topo->tiles[ pohh_id ].pohh.bundle.enabled==crank_enabled );
  FD_TEST( (fd_topo_find_tile_obj( topo, &topo->tiles[ pohh_id ], "bam_ctrl" )!=NULL)==bam_enabled );

  FD_TEST( (fd_topo_find_tile( topo, "bundle", 0UL )!=ULONG_MAX)==bundle_enabled );
  FD_TEST( (fd_topo_find_tile( topo, "bam",    0UL )!=ULONG_MAX)==bam_enabled );

  int bam_plugin_enabled = bam_enabled && gui_enabled;
  ulong bam_plugi_link_id = fd_topo_find_link( topo, "bam_plugi", 0UL );
  FD_TEST( (bam_plugi_link_id!=ULONG_MAX)==bam_plugin_enabled );
  if( FD_UNLIKELY( bam_plugin_enabled ) ) {
    ulong bam_id    = fd_topo_find_tile( topo, "bam",    0UL );
    ulong plugin_id = fd_topo_find_tile( topo, "plugin", 0UL );
    FD_TEST( bam_id!=ULONG_MAX && plugin_id!=ULONG_MAX );
    fd_topo_tile_t const * bam    = &topo->tiles[ bam_id ];
    fd_topo_tile_t const * plugin = &topo->tiles[ plugin_id ];
    ulong bam_out_idx    = fd_topo_find_tile_out_link( topo, bam,    "bam_plugi", 0UL );
    ulong plugin_in_idx  = fd_topo_find_tile_in_link ( topo, plugin, "bam_plugi", 0UL );
    FD_TEST( bam_out_idx!=ULONG_MAX && bam->out_link_id[ bam_out_idx ]==bam_plugi_link_id );
    FD_TEST( plugin_in_idx!=ULONG_MAX && plugin->in_link_id[ plugin_in_idx ]==bam_plugi_link_id );
    FD_TEST( plugin->in_link_reliable[ plugin_in_idx ] && plugin->in_link_poll[ plugin_in_idx ] );
  }
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  test_topology( 0, 0, 0 );
  test_topology( 1, 0, 0 );
  test_topology( 0, 1, 0 );
  test_topology( 1, 1, 0 );
  test_topology( 0, 1, 1 );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
