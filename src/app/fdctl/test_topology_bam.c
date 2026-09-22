#include "commands/set_identityh.c"
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
STUB_TILE( waker  );

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
  &stub_tile_waker,
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

/* Frankendancer's set-identity halts bundle and BAM signing together
   with the leader pipeline, switches every identity tile, and resumes
   the leader pipeline only after bundle and BAM resumed.  Tiles answer
   one request per poll so an ordering slip is observable. */

static int
set_identity_tile_halts( char const * name ) {
  return !strcmp( name, "pohh" ) || !strcmp( name, "bundle" ) || !strcmp( name, "bam" );
}

static void
test_set_identity( fd_topo_t * topo ) {
  static uchar ks_mem[ (FD_TOPO_MAX_TILES+1UL)*FD_KEYSWITCH_FOOTPRINT ] __attribute__((aligned(FD_KEYSWITCH_ALIGN)));
  static int   halted  [ FD_TOPO_MAX_TILES ];
  static ulong switched[ FD_TOPO_MAX_TILES ];

  for( ulong i=0UL; i<topo->tile_cnt; i++ ) {
    fd_topo_tile_t const * tile = &topo->tiles[ i ];
    halted  [ i ] = 0;
    switched[ i ] = 0UL;
    if( tile->id_keyswitch_obj_id==ULONG_MAX ) continue;
    fd_topo_obj_t * obj = &topo->objs[ tile->id_keyswitch_obj_id ];
    topo->workspaces[ obj->wksp_id ].wksp = (fd_wksp_t *)ks_mem;
    obj->offset = (i+1UL)*FD_KEYSWITCH_FOOTPRINT;
    ulong state = !strcmp( tile->name, "pohh" ) ? FD_KEYSWITCH_STATE_UNLOCKED : FD_KEYSWITCH_STATE_COMPLETED;
    FD_TEST( fd_keyswitch_join( fd_keyswitch_new( ks_mem+obj->offset, state ) ) );
  }

  uchar new_key[ 64 ];
  for( ulong i=0UL; i<64UL; i++ ) new_key[ i ] = (uchar)(i+1UL);
  uchar * keypair_wr = fd_keyload_alloc_protected_pages( 1UL, 1UL ); /* set-identity wipes the private half */
  fd_memcpy( keypair_wr, new_key, 64UL );
  uchar const * keypair = fd_keyload_mprotect_ro( keypair_wr, 0 );

  ulong pohh_idx   = fd_topo_find_tile( topo, "pohh", 0UL );
  ulong state      = FD_SET_IDENTITY_STATE_UNLOCKED;
  ulong halted_seq = 0UL;
  int   has_error  = 0;
  int   done       = 0;
  for( ulong iter=0UL; iter<1024UL && !done; iter++ ) {
    ulong prev = state;
    poll_keyswitch( topo, &state, &halted_seq, keypair, &has_error, 0, 0, 0 );
    done = state==FD_SET_IDENTITY_STATE_UNLOCKED;
    if( done || state!=prev ) continue;
    for( ulong i=0UL; i<topo->tile_cnt; i++ ) {
      fd_topo_tile_t const * tile = &topo->tiles[ i ];
      if( tile->id_keyswitch_obj_id==ULONG_MAX ) continue;
      fd_keyswitch_t * ks = fd_topo_obj_laddr( topo, tile->id_keyswitch_obj_id );
      ulong ks_state = fd_keyswitch_state_query( ks );
      if( ks_state==FD_KEYSWITCH_STATE_SWITCH_PENDING ) {
        int is_sign = !strcmp( tile->name, "sign" );
        int full    = is_sign || i==pohh_idx;
        FD_TEST( full ? fd_memeq( ks->bytes, new_key, 64UL ) : fd_memeq( ks->bytes, new_key+32UL, 32UL ) );
        for( ulong j=0UL; is_sign && j<topo->tile_cnt; j++ ) {
          fd_topo_tile_t const * other = &topo->tiles[ j ];
          if( other->id_keyswitch_obj_id==ULONG_MAX ) continue;
          if( FD_UNLIKELY( ( set_identity_tile_halts( other->name ) || !strcmp( other->name, "shred" ) ) && !switched[ j ] ) )
            FD_LOG_ERR(( "sign switched before %s", other->name ));
        }
        switched[ i ]++;
        halted  [ i ] = set_identity_tile_halts( tile->name );
        ks->result    = i;
        fd_keyswitch_state( ks, FD_KEYSWITCH_STATE_COMPLETED );
        break;
      } else if( ks_state==FD_KEYSWITCH_STATE_UNHALT_PENDING ) {
        if( FD_UNLIKELY( !set_identity_tile_halts( tile->name ) ) ) FD_LOG_ERR(( "set-identity asked %s to unhalt", tile->name ));
        for( ulong j=0UL; i==pohh_idx && j<topo->tile_cnt; j++ ) {
          if( FD_UNLIKELY( j!=i && halted[ j ] ) ) FD_LOG_ERR(( "leader pipeline resumed before %s", topo->tiles[ j ].name ));
        }
        halted[ i ] = 0;
        fd_keyswitch_state( ks, FD_KEYSWITCH_STATE_COMPLETED );
        break;
      }
    }
  }
  if( FD_UNLIKELY( !done ) ) FD_LOG_ERR(( "set-identity stuck in state %lu", state ));
  FD_TEST( !has_error );
  FD_TEST( halted_seq==pohh_idx );

  for( ulong i=0UL; i<topo->tile_cnt; i++ ) {
    fd_topo_tile_t const * tile = &topo->tiles[ i ];
    if( tile->id_keyswitch_obj_id==ULONG_MAX ) continue;
    if( FD_UNLIKELY( switched[ i ]!=1UL ) ) FD_LOG_ERR(( "set-identity switched %s %lu times", tile->name, switched[ i ] ));
    if( FD_UNLIKELY( halted[ i ] ) ) FD_LOG_ERR(( "set-identity left %s halted", tile->name ));
    ulong ks_state = fd_keyswitch_state_query( fd_topo_obj_laddr( topo, tile->id_keyswitch_obj_id ) );
    FD_TEST( ks_state==( i==pohh_idx ? FD_KEYSWITCH_STATE_UNLOCKED : FD_KEYSWITCH_STATE_COMPLETED ) );
  }
}

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
  if( bam_enabled ) {
    /* Frankendancer has no efficient layout, so BAM polls its socket. */
    fd_topo_tile_t const * bam = &topo->tiles[ fd_topo_find_tile( topo, "bam", 0UL ) ];
    ulong replay_in = fd_topo_find_tile_in_link( topo, bam, "replay_out", 0UL );
    FD_TEST( replay_in!=ULONG_MAX );
    FD_TEST( bam->in_link_poll[ replay_in ] && !bam->in_link_reliable[ replay_in ] );
    FD_TEST( fd_topo_find_tile_in_link( topo, bam, "replay_slot", 0UL )==ULONG_MAX );
    FD_TEST( !bam->is_waker_client && bam->waker_client_idx==ULONG_MAX );
    ulong verify_link = fd_topo_find_link( topo, "bam_verif", 0UL );
    ulong sign_link   = fd_topo_find_link( topo, "bam_sign",  0UL );
    FD_TEST( verify_link!=ULONG_MAX && sign_link!=ULONG_MAX );
    FD_TEST( fd_topo_link_consumer_cnt( topo, &topo->links[ verify_link ] )==1UL );
    fd_topo_tile_t const * verify = &topo->tiles[ fd_topo_find_tile( topo, "verify", 0UL ) ];
    ulong verify_in = fd_topo_find_tile_in_link( topo, verify, "bam_verif", 0UL );
    FD_TEST( verify_in!=ULONG_MAX && verify->in_link_reliable[ verify_in ] && verify->in_link_poll[ verify_in ] );
  }

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

  test_set_identity( &config->topo );
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
