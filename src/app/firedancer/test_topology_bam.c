#include "topology.h"
#include "config.h"
#include "../shared/fd_config_private.h"
#include "../shared/fd_action.h"
#include "../../util/pod/fd_pod_format.h"
#include "../../flamenco/accdb/fd_accdb_cache.h"
#include "../../disco/bam/fd_bam_types.h"
#include "../../disco/bundle/fd_bundle_crank.h"
#include "../../disco/keyguard/fd_keyguard.h"
#include "../../disco/pack/fd_pack_cost.h"
#include "../../disco/topo/fd_topob_layout.h"
#include "../../ballet/shred/fd_shred.h"
#include "../../discof/admin/fd_admin_tile.c"

char const * FD_APP_NAME    = "test_firedancer_topology_bam";
char const * FD_BINARY_NAME = "test_firedancer_topology_bam";

action_t * ACTIONS[] = { NULL };

/* Callbacks for the object types fd_topo_initialize creates. */
#define TEST_OBJ_CALLBACKS( X )                                                                                  \
  X( mcache ) X( dcache ) X( fseq ) X( metrics ) X( netdev_tbl ) X( neigh4_hmap ) X( keyswitch ) X( node_info ) \
  X( wait_info ) X( leader_txn_timing ) X( bam_ctrl ) X( bam_fee_cfg ) X( tile ) X( store ) X( fec_sets )       \
  X( txncache ) X( accdb ) X( backup ) X( banks ) X( progcache ) X( rnonce_ss ) X( footer_cache ) X( adminctl ) X( sleep )
#define TEST_OBJ_CB_EXTERN( name ) extern fd_topo_obj_callbacks_t fd_obj_cb_##name;
#define TEST_OBJ_CB_REF( name )    &fd_obj_cb_##name,
TEST_OBJ_CALLBACKS( TEST_OBJ_CB_EXTERN )
fd_topo_obj_callbacks_t * CALLBACKS[] = { TEST_OBJ_CALLBACKS( TEST_OBJ_CB_REF ) NULL };

static ulong stub_tile_scratch_align    ( void )                        { return 1UL; }
static ulong stub_tile_scratch_footprint( fd_topo_tile_t const * tile ) { (void)tile; return 1UL; }

/* fd_topo_initialize sizes every tile it creates through TILES; main
   fills these stubs in from the names. */
static char const * const stub_tile_names[] = {
  "net", "netlnk", "sock", "quic", "verify", "dedup", "resolv", "pack", "execle", "shred", "sign", "metric",
  "event", "diag", "gui", "rpc", "bundle", "bam", "gossvf", "gossip", "rserve", "replay", "execrp",
  "motor", "rotor", "votor", "accdb", "snapct", "snapld", "snapdc", "snapin", "snapwr",
  "genesi", "ipecho", "admin", "solcap", "snapmk", "snapzp", "snaprd", "snapsv", "waker", "mwaitx"
};
#define STUB_TILE_CNT (sizeof(stub_tile_names)/sizeof(stub_tile_names[0]))
static fd_topo_run_tile_t stub_tiles[ STUB_TILE_CNT ];
fd_topo_run_tile_t *      TILES     [ STUB_TILE_CNT+1UL ];

/* Drive the admin tile's set-identity state machine over a real
   topology.  Each identity keyswitch user answers the way its
   during_housekeeping does: "halt" tiles stop signing on SWITCH_PENDING
   and resume on UNHALT_PENDING, the rest only complete SWITCH_PENDING.
   Tiles answer one at a time and only while the admin waits, so a
   missing wait lets the admin run ahead of a slow tile.  Every user
   must be switched exactly once to the new key, every halting signer
   and shred must switch before the sign tile does, and every halting
   signer must resume before the leader pipeline does.  Asking a tile
   to unhalt that never handles UNHALT_PENDING hangs set-identity
   forever, and a tile left halted stops signing.  An unlisted
   keyswitch user fails the test so it gets classified. */

static int
set_identity_tile_halts( char const * name ) {
  static char const * const halt[] = { "replay", "rotor", "gossip", "votor", "bundle", "rserve", "bam" };
  static char const * const once[] = { "shred", "sign", "gossvf", "pack", "rpc", "event", "gui" };
  for( ulong i=0UL; i<sizeof(halt)/sizeof(halt[0]); i++ ) if( !strcmp( name, halt[i] ) ) return 1;
  for( ulong i=0UL; i<sizeof(once)/sizeof(once[0]); i++ ) if( !strcmp( name, once[i] ) ) return 0;
  FD_LOG_ERR(( "unmodeled identity keyswitch user %s", name ));
}

static void
test_set_identity( fd_topo_t * topo ) {
  static uchar ks_mem[ (FD_TOPO_MAX_TILES+1UL)*FD_KEYSWITCH_FOOTPRINT ] __attribute__((aligned(FD_KEYSWITCH_ALIGN)));
  static int   halted  [ FD_TOPO_MAX_TILES ];
  static ulong switched[ FD_TOPO_MAX_TILES ];
  static fd_admin_tile_ctx_t admin[1];
  fd_memset( admin, 0, sizeof(admin) );
  admin->topo = topo;
  ulong voter_idx = fd_topo_find_tile( topo, "votor", 0UL );
  FD_TEST( voter_idx!=ULONG_MAX );

  for( ulong i=0UL; i<topo->tile_cnt; i++ ) {
    fd_topo_tile_t const * tile = &topo->tiles[ i ];
    halted  [ i ] = 0;
    switched[ i ] = 0UL;
    if( tile->id_keyswitch_obj_id==ULONG_MAX ) continue;
    set_identity_tile_halts( tile->name );
    fd_topo_obj_t * obj = &topo->objs[ tile->id_keyswitch_obj_id ];
    topo->workspaces[ obj->wksp_id ].wksp = (fd_wksp_t *)ks_mem;
    obj->offset = (i+1UL)*FD_KEYSWITCH_FOOTPRINT;
    ulong state = !strcmp( tile->name, "replay" ) ? FD_KEYSWITCH_STATE_UNLOCKED : FD_KEYSWITCH_STATE_COMPLETED;
    FD_TEST( fd_keyswitch_join( fd_keyswitch_new( ks_mem+obj->offset, state ) ) );
  }

  uchar new_key[ 64 ];
  for( ulong i=0UL; i<64UL; i++ ) new_key[ i ] = (uchar)(i+1UL);
  uchar keypair[ 64 ]; /* the admin tile wipes the private half */
  fd_memcpy( keypair, new_key, 64UL );
  ulong state      = FD_SET_IDENTITY_STATE_UNLOCKED;
  int   done       = 0;
  for( ulong iter=0UL; iter<1024UL && !done; iter++ ) {
    ulong prev = state;
    done = poll_set_identity( admin, &state, 1UL, keypair, 0UL );
    if( state!=prev ) continue;
    for( ulong i=0UL; i<topo->tile_cnt; i++ ) {
      fd_topo_tile_t const * tile = &topo->tiles[ i ];
      if( tile->id_keyswitch_obj_id==ULONG_MAX ) continue;
      fd_keyswitch_t * ks = fd_topo_obj_laddr( topo, tile->id_keyswitch_obj_id );
      ulong ks_state = fd_keyswitch_state_query( ks );
      if( ks_state==FD_KEYSWITCH_STATE_SWITCH_PENDING ) {
        int is_sign = !strcmp( tile->name, "sign" );
        FD_TEST( is_sign ? fd_memeq( ks->bytes, new_key, 64UL ) : fd_memeq( ks->bytes, new_key+32UL, 32UL ) );
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
        for( ulong j=0UL; !strcmp( tile->name, "replay" ) && j<topo->tile_cnt; j++ ) {
          if( FD_UNLIKELY( j!=i && halted[ j ] ) ) FD_LOG_ERR(( "leader pipeline resumed before %s", topo->tiles[ j ].name ));
        }
        halted[ i ] = 0;
        fd_keyswitch_state( ks, FD_KEYSWITCH_STATE_COMPLETED );
        break;
      }
    }
  }
  if( FD_UNLIKELY( !done ) ) FD_LOG_ERR(( "set-identity stuck in state %lu", state ));
  fd_keyswitch_t const * voter = fd_topo_obj_laddr( topo, topo->tiles[ voter_idx ].id_keyswitch_obj_id );
  FD_TEST( fd_keyswitch_param_query( voter )==fd_topo_find_tile( topo, "replay", 0UL ) );

  for( ulong i=0UL; i<topo->tile_cnt; i++ ) {
    fd_topo_tile_t const * tile = &topo->tiles[ i ];
    if( tile->id_keyswitch_obj_id==ULONG_MAX ) continue;
    if( FD_UNLIKELY( switched[ i ]!=1UL ) ) FD_LOG_ERR(( "set-identity switched %s %lu times", tile->name, switched[ i ] ));
    if( FD_UNLIKELY( halted[ i ] ) ) FD_LOG_ERR(( "set-identity left %s halted", tile->name ));
    ulong ks_state = fd_keyswitch_state_query( fd_topo_obj_laddr( topo, tile->id_keyswitch_obj_id ) );
    FD_TEST( ks_state==( !strcmp( tile->name, "replay" ) ? FD_KEYSWITCH_STATE_UNLOCKED : FD_KEYSWITCH_STATE_COMPLETED ) );
  }
}

/* A tile joins a workspace writable if it uses any object in it
   writable.  Only the tiles that own BAM control state may do so for
   the BAM control workspaces: pack acknowledges ownership generations
   in bam_status.  A writable mapping elsewhere (e.g. gossip, which
   parses untrusted network input, or bundle, which only reads the
   override) could flip the override or scramble the ownership
   generation. */

static void
test_bam_control_wksp_writers( fd_topo_t const * topo ) {
  static char const * const wksps[] = { "bam_status", "bam_ctrl", "bam_fee_cfg" };
  for( ulong i=0UL; i<topo->tile_cnt; i++ ) {
    fd_topo_tile_t const * tile = &topo->tiles[ i ];
    for( ulong j=0UL; j<tile->uses_obj_cnt; j++ ) {
      if( tile->uses_obj_mode[ j ]!=FD_SHMEM_JOIN_MODE_READ_WRITE ) continue;
      char const * wksp = topo->workspaces[ topo->objs[ tile->uses_obj_id[ j ] ].wksp_id ].name;
      for( ulong k=0UL; k<sizeof(wksps)/sizeof(wksps[0]); k++ ) {
        if( strcmp( wksp, wksps[ k ] ) ) continue;
        int owner = !strcmp( tile->name, "bam" ) ||
                    ( !strcmp( wksp, "bam_status" ) && !strcmp( tile->name, "pack" ) );
        if( FD_UNLIKELY( !owner ) ) FD_LOG_ERR(( "%s:%lu maps BAM control workspace %s writable", tile->name, tile->kind_id, wksp ));
      }
    }
  }
}

/* Use the generator and the actual sign authorization boundary together.
   Program authorities come from the configured sign tile, while generation
   uses the pack tile's configuration.  Identity/vote/builder keys are
   non-secret fixtures; the topology stores key paths rather than key bytes. */
static void
test_generated_crank_authorization( fd_topo_tile_t const * pack,
                                    fd_topo_tile_t const * sign ) {
  fd_acct_addr_t tip_distribution, tip_payment, merkle_authority;
  fd_memcpy( tip_distribution.b, pack->pack.bundle.tip_distribution_program_addr, 32UL );
  fd_memcpy( tip_payment.b,      pack->pack.bundle.tip_payment_program_addr,      32UL );
  fd_memcpy( merkle_authority.b, pack->pack.bundle.tip_distribution_authority,   32UL );
  fd_acct_addr_t vote = { .b={7} };
  fd_acct_addr_t builder = { .b={8} };
  fd_acct_addr_t identity = { .b={6} };

  fd_keyguard_authority_t authority = {0};
  fd_memcpy( authority.identity_pubkey, identity.b, 32UL );
  fd_memcpy( authority.tip_distribution_program, sign->sign.bundle.tip_distribution_program_addr, 32UL );
  fd_memcpy( authority.tip_payment_program,      sign->sign.bundle.tip_payment_program_addr,      32UL );
  for( int create=0; create<2; create++ ) {
    fd_bundle_crank_gen_t gen[1];
    FD_TEST( fd_bundle_crank_gen_init( gen, &tip_distribution, &tip_payment, &vote,
                                      &merkle_authority, "NONE", pack->pack.bundle.commission_bps ) );
    fd_bundle_crank_tip_payment_config_t old = { .discriminator=0x82ccfa1ee0aa0c9bUL };
    old.tip_receiver->b[0] = 9;
    old.block_builder->b[0] = 10;
    fd_acct_addr_t owner = create ? (fd_acct_addr_t){ .b={0} } : tip_distribution;
    uchar payload[FD_TXN_MTU];
    uchar txn_buf[FD_TXN_MAX_SZ] __attribute__((aligned(8)));
    fd_txn_t * txn = (fd_txn_t *)txn_buf;
    ulong payload_sz = fd_bundle_crank_generate( gen, &old, &builder, &identity, &owner,
                                                740UL, 19UL, payload, txn );
    FD_TEST( payload_sz==(create ? FD_BUNDLE_CRANK_3_SZ : FD_BUNDLE_CRANK_2_SZ) );
    FD_TEST( txn->signature_cnt==1U && txn->message_off==65U );
    uchar const * message = payload+txn->message_off;
    ulong message_sz = fd_txn_msg_sz( txn, payload_sz );
    FD_TEST( fd_keyguard_payload_authorize( &authority, message, message_sz,
                                            FD_KEYGUARD_ROLE_BUNDLE_CRANK, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
    FD_TEST( !fd_keyguard_payload_authorize( &authority, message, message_sz,
                                             FD_KEYGUARD_ROLE_BAM, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
    fd_keyguard_authority_t bad[] = { authority, authority, authority };
    bad[0].identity_pubkey[0] ^= 1U;
    bad[1].tip_payment_program[0] ^= 1U;
    bad[2].tip_distribution_program[0] ^= 1U;
    /* Only the create-account crank invokes the distribution program. */
    for( ulong i=0UL; i<(create ? 3UL : 2UL); i++ )
      FD_TEST( !fd_keyguard_payload_authorize( &bad[i], message, message_sz,
                                               FD_KEYGUARD_ROLE_BUNDLE_CRANK, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
  }
}

/* Builds the topology of the default config with the given modes. */
static config_t *
topology_config( int          bundle_enabled,
                 int          bam_enabled,
                 int          leader_enabled,
                 char const * layout_mode ) {
  static config_t config[1];
  fd_memset( config, 0, sizeof(config_t) );
  fd_config_load_buf( config, (char const *)firedancer_default_config,
                     firedancer_default_config_sz, "default.toml" );
  fd_config_validate( config );
  /* fd_config_load_buf parses TOML only.  Normal startup computes these
     derived limits after validation, before building the topology. */
  FD_TEST( !config->development.bench.max_cost_per_block && !config->development.bench.max_shreds_per_block );
  config->limits.max_cost_per_block   = FD_PACK_MAX_COST_PER_BLOCK_UPPER_BOUND;
  config->limits.max_shreds_per_block = FD_SHRED_BLK_MAX;
  config->limits.max_txn_per_slot     = FD_MAX_TXN_PER_SLOT;

  fd_cstr_ncpy( config->net.provider, "socket", sizeof(config->net.provider) );
  config->telemetry = 0;
  config->tiles.gui.enabled = 0;
  config->tiles.bundle.enabled = bundle_enabled;
  config->bam.tiles.enabled = bam_enabled;
  config->firedancer.layout.enable_block_production = leader_enabled;

  fd_cstr_ncpy( config->net.interface, "lo", sizeof(config->net.interface) );
  fd_cstr_ncpy( config->layout.affinity, "f128", sizeof(config->layout.affinity) );
  fd_cstr_ncpy( config->firedancer.layout.mode, layout_mode, sizeof(config->firedancer.layout.mode) );
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
  return config;
}

/* Exercise every BAM/bundle mode: missing shared crank/sign wiring breaks
   BAM-only config and identity updates, while an unwanted source tile leaves
   a disabled ingress path active. */
static void
test_topology( int          bundle_enabled,
               int          bam_enabled,
               int          leader_enabled,
               char const * layout_mode ) {
  config_t * config = topology_config( bundle_enabled, bam_enabled, leader_enabled, layout_mode );

  int crank_enabled = bundle_enabled || bam_enabled;
  fd_topo_t const * topo = &config->topo;
  if( !leader_enabled ) {
    /* Without block production, bundle/BAM settings must not leave
       their tiles, links or crank configuration behind. */
    static char const * const bam_links[] = { "pack_sign", "sign_pack", "bam_verif", "bam_sign", "bam_gossip", "bam_shred", "executed_txn", "pack_bam_ldr", "pack_bam_res", "bank_bam", "motor_bam" };
    FD_TEST( fd_topo_find_tile( topo, "bundle", 0UL )==ULONG_MAX );
    FD_TEST( fd_topo_find_tile( topo, "bam",    0UL )==ULONG_MAX );
    for( ulong i=0UL; i<sizeof(bam_links)/sizeof(bam_links[0]); i++ )
      FD_TEST( fd_topo_find_link( topo, bam_links[ i ], 0UL )==ULONG_MAX );
    FD_TEST( !topo->tiles[ fd_topo_find_tile( topo, "replay", 0UL ) ].replay.bundle.enabled );
    return;
  }

  ulong pack_id = fd_topo_find_tile( topo, "pack", 0UL );
  FD_TEST( pack_id!=ULONG_MAX );
  fd_topo_tile_t const * pack = &topo->tiles[ pack_id ];

  FD_TEST( (pack->id_keyswitch_obj_id!=ULONG_MAX)==crank_enabled );
  FD_TEST( (fd_topo_find_link( topo, "pack_sign", 0UL )!=ULONG_MAX)==crank_enabled );
  FD_TEST( (fd_topo_find_link( topo, "sign_pack", 0UL )!=ULONG_MAX)==crank_enabled );
  FD_TEST( !!pack->pack.bundle.enabled==crank_enabled );
  FD_TEST( !!pack->pack.bam_enabled==!!bam_enabled ); /* sizes pack's BAM tracking */

  ulong sign_id = fd_topo_find_tile( topo, "sign", 0UL );
  FD_TEST( sign_id!=ULONG_MAX );
  fd_topo_tile_t const * sign = &topo->tiles[ sign_id ];
  FD_TEST( sign->id_keyswitch_obj_id!=ULONG_MAX );
  /* Sign publishes to the same numeric index as its input.  Changing
     sign clients must preserve the request/response pairing for
     BAM authentication and the shared crank in every mode. */
  FD_TEST( sign->in_cnt==sign->out_cnt );
  char const * sign_in [] = { "bam_sign", "pack_sign", "bundle_sign" };
  char const * sign_out[] = { "sign_bam", "sign_pack", "sign_bundle" };
  int          enabled [] = { bam_enabled, crank_enabled, bundle_enabled };
  for( ulong i=0UL; i<3UL; i++ ) {
    ulong in  = fd_topo_find_tile_in_link ( topo, sign, sign_in [ i ], 0UL );
    ulong out = fd_topo_find_tile_out_link( topo, sign, sign_out[ i ], 0UL );
    FD_TEST( (in!=ULONG_MAX)==enabled[ i ] && in==out );
    if( in!=ULONG_MAX ) FD_TEST( sign->in_link_poll[ in ] );
  }
  for( ulong i=0UL; i<fd_topo_tile_name_cnt( topo, "shred" ); i++ ) {
    ulong shred_id = fd_topo_find_tile( topo, "shred", i );
    FD_TEST( shred_id!=ULONG_MAX && topo->tiles[ shred_id ].id_keyswitch_obj_id!=ULONG_MAX );
    /* fd_shred_tile indexes in_kind by tile in index, so every in
       (including bam_shred) must be polled */
    fd_topo_tile_t const * shred = &topo->tiles[ shred_id ];
    for( ulong j=0UL; j<shred->in_cnt; j++ ) FD_TEST( shred->in_link_poll[ j ] );
  }
  uchar disabled_program[ 32 ];
  fd_memset( disabled_program, 0xFF, sizeof(disabled_program) );
  FD_TEST( fd_memeq( sign->sign.bundle.tip_distribution_program_addr,
                     crank_enabled ? pack->pack.bundle.tip_distribution_program_addr : disabled_program, 32UL ) );
  FD_TEST( fd_memeq( sign->sign.bundle.tip_payment_program_addr,
                     crank_enabled ? pack->pack.bundle.tip_payment_program_addr : disabled_program, 32UL ) );
  if( crank_enabled ) test_generated_crank_authorization( pack, sign );

  ulong replay_id = fd_topo_find_tile( topo, "replay", 0UL );
  FD_TEST( replay_id!=ULONG_MAX );
  FD_TEST( !!topo->tiles[ replay_id ].replay.bundle.enabled==crank_enabled );
  FD_TEST( !fd_topo_find_tile_obj( topo, &topo->tiles[ replay_id ], "bam_ctrl" ) );

  FD_TEST( (fd_topo_find_tile( topo, "bundle", 0UL )!=ULONG_MAX)==bundle_enabled );
  FD_TEST( (fd_topo_find_tile( topo, "bam",    0UL )!=ULONG_MAX)==bam_enabled );
  if( bam_enabled ) {
    fd_topo_tile_t const * bam = &topo->tiles[ fd_topo_find_tile( topo, "bam", 0UL ) ];
    /* BAM consumes terminal worker results as one contiguous polled
       block, followed immediately by the Motor result input. */
    ulong worker_cnt = fd_topo_tile_name_cnt( topo, "execle" );
    ulong bank_in = fd_topo_find_tile_in_link( topo, bam, "bank_bam", 0UL );
    FD_TEST( worker_cnt && bank_in!=ULONG_MAX );
    for( ulong i=0UL; i<worker_cnt; i++ ) {
      ulong in = fd_topo_find_tile_in_link( topo, bam, "bank_bam", i );
      FD_TEST( in==bank_in+i && bam->in_link_poll[ in ] && !bam->in_link_reliable[ in ] );
      fd_topo_tile_t const * worker = &topo->tiles[ fd_topo_find_tile( topo, "execle", i ) ];
      ulong out = fd_topo_find_tile_out_link( topo, worker, "bank_bam", i );
      FD_TEST( out!=ULONG_MAX && worker->out_link_id[ out ]==bam->in_link_id[ in ] );
    }
    ulong motor_in = fd_topo_find_tile_in_link( topo, bam, "motor_bam", 0UL );
    FD_TEST( motor_in==bank_in+worker_cnt && bam->in_link_poll[ motor_in ] && !bam->in_link_reliable[ motor_in ] );
    /* Result feedback never backpressures its producers. */
    ulong pack_res_in = fd_topo_find_tile_in_link( topo, bam, "pack_bam_res", 0UL );
    FD_TEST( pack_res_in!=ULONG_MAX && bam->in_link_poll[ pack_res_in ] && !bam->in_link_reliable[ pack_res_in ] );
    ulong result_cnt = 0UL;
    for( ulong i=0UL; i<bam->in_cnt; i++ ) {
      fd_topo_link_t const * link = &topo->links[ bam->in_link_id[ i ] ];
      int is_result = !strcmp( link->name, "pack_bam_res" ) || !strcmp( link->name, "bank_bam" ) || !strcmp( link->name, "motor_bam" );
      FD_TEST( !is_result || link->depth==FD_BAM_RESULT_LINK_DEPTH );
      result_cnt += !strcmp( link->name, "bank_bam" ) || !strcmp( link->name, "motor_bam" );
    }
    FD_TEST( result_cnt==worker_cnt+1UL );
    ulong replay_in = fd_topo_find_tile_in_link( topo, bam, "replay_slot", 0UL );
    FD_TEST( replay_in!=ULONG_MAX );
    ulong sign_in = fd_topo_find_tile_in_link( topo, bam, "sign_bam", 0UL );
    FD_TEST( sign_in==bam->in_cnt-1UL && !bam->in_link_poll[ sign_in ] );
    for( ulong i=0UL; i<sign_in; i++ ) FD_TEST( bam->in_link_poll[ i ] );
    FD_TEST( bam->in_link_poll[ replay_in ] && !bam->in_link_reliable[ replay_in ] );
    FD_TEST( fd_topo_find_tile_in_link( topo, bam, "replay_out", 0UL )==ULONG_MAX );
    /* Only efficient layout parks tiles; performance layout polls the
       BAM socket instead of waiting for the waker tile. */
    FD_TEST( bam->id_keyswitch_obj_id!=ULONG_MAX );
    FD_TEST( bam->is_waker_client==!strcmp( layout_mode, "efficient" ) );
    FD_TEST( (bam->waker_client_idx!=ULONG_MAX)==bam->is_waker_client );
    /* fd_topob_layout.c floats unlisted tiles; BAM is placed like bundle. */
    FD_TEST( fd_topob_tile_class( "bam" )==FD_TOPOB_TILE_ALWAYS );
    FD_TEST( fd_topob_tile_class( "bam" )==fd_topob_tile_class( "bundle" ) );
    ulong signed_obj_id = fd_pod_query_ulong( topo->props, "bam_gossip_signed", ULONG_MAX );
    FD_TEST( signed_obj_id<topo->obj_cnt );
    fd_topo_tile_t const * gossip = &topo->tiles[ fd_topo_find_tile( topo, "gossip", 0UL ) ];
    int bam_reads = 0;
    int gossip_writes = 0;
    for( ulong i=0UL; i<bam->uses_obj_cnt; i++ )
      bam_reads |= bam->uses_obj_id[i]==signed_obj_id && bam->uses_obj_mode[i]==FD_SHMEM_JOIN_MODE_READ_ONLY;
    for( ulong i=0UL; i<gossip->uses_obj_cnt; i++ )
      gossip_writes |= gossip->uses_obj_id[i]==signed_obj_id && gossip->uses_obj_mode[i]==FD_SHMEM_JOIN_MODE_READ_WRITE;
    FD_TEST( bam_reads && gossip_writes );
    test_bam_control_wksp_writers( topo );
    if( bundle_enabled ) {
      ulong status_obj_id = fd_pod_query_ulong( topo->props, "bam_status", ULONG_MAX );
      fd_topo_tile_t const * bundle = &topo->tiles[ fd_topo_find_tile( topo, "bundle", 0UL ) ];
      int bundle_reads = 0;
      for( ulong i=0UL; i<bundle->uses_obj_cnt; i++ )
        bundle_reads |= bundle->uses_obj_id[i]==status_obj_id && bundle->uses_obj_mode[i]==FD_SHMEM_JOIN_MODE_READ_ONLY;
      FD_TEST( status_obj_id<topo->obj_cnt && bundle_reads );
    }
    ulong verify_link = fd_topo_find_link( topo, "bam_verif", 0UL );
    ulong sign_link   = fd_topo_find_link( topo, "bam_sign",  0UL );
    FD_TEST( verify_link!=ULONG_MAX && sign_link!=ULONG_MAX );
    FD_TEST( fd_topo_link_consumer_cnt( topo, &topo->links[ verify_link ] )==1UL );
    fd_topo_tile_t const * verify = &topo->tiles[ fd_topo_find_tile( topo, "verify", 0UL ) ];
    ulong verify_in = fd_topo_find_tile_in_link( topo, verify, "bam_verif", 0UL );
    FD_TEST( verify_in!=ULONG_MAX && verify->in_link_reliable[ verify_in ] && verify->in_link_poll[ verify_in ] );
  }

  /* executed_txn lets pack retire BAM work.  Upstream Firedancer has
     no such link, so without BAM leaders must not pay its
     per-transaction publish.  It carries only BAM completions, so dedup
     (which BAM bypasses) does not consume it. */
  FD_TEST( (fd_topo_find_link( topo, "executed_txn", 0UL )!=ULONG_MAX)==bam_enabled );
  fd_topo_tile_t const * pack_tile  = &topo->tiles[ fd_topo_find_tile( topo, "pack",  0UL ) ];
  fd_topo_tile_t const * dedup_tile = &topo->tiles[ fd_topo_find_tile( topo, "dedup", 0UL ) ];
  FD_TEST( (fd_topo_find_tile_in_link( topo, pack_tile,  "executed_txn", 0UL )!=ULONG_MAX)==bam_enabled );
  FD_TEST( fd_topo_find_tile_in_link( topo, dedup_tile, "executed_txn", 0UL )==ULONG_MAX );

  ulong motor_id = fd_topo_find_tile( topo, "motor", 0UL );
  FD_TEST( motor_id!=ULONG_MAX );
  FD_TEST( (fd_topo_find_tile_out_link( topo, &topo->tiles[ motor_id ], "executed_txn", 0UL )!=ULONG_MAX)==bam_enabled );
  FD_TEST( (fd_topo_find_tile_out_link( topo, &topo->tiles[ motor_id ], "motor_bam", 0UL )!=ULONG_MAX)==bam_enabled );

  fd_topo_obj_t const * accdb = fd_topo_find_obj( topo, "accdb", NULL, ULONG_MAX );
  FD_TEST( accdb );
  FD_TEST( fd_pod_queryf_ulong( topo->props, 0UL, "obj.%lu.cache_min_reserved", accdb->id )==
           fd_accdb_cache_min_reserved( crank_enabled ) );

  test_set_identity( &config->topo );
}

/* On hosts short of physical cores (24 and 32 here, performance mode)
   the auto layout shares some cores.  With BAM and bundle on, bam
   shares one with bundle (fd_topo_bam_smt_pair), which sleeps while
   BAM is active, instead of with a busy tile such as dedup or net.
   Single socket hosts, siblings c and c+phys, with the default
   blocklist "0h" (CPUs 0 and phys). */

static void
test_bam_smt_sibling( ulong phys ) {
  FD_TEST( fd_topo_bam_smt_pair( "bundle", "bam" ) && fd_topo_bam_smt_pair( "bam", "bundle" ) );
  FD_TEST( !fd_topo_bam_smt_pair( "bam", "bam" ) && !fd_topo_bam_smt_pair( "bundle", "bundle" ) );
  FD_TEST( !fd_topo_bam_smt_pair( "bam", "dedup" ) && !fd_topo_bam_smt_pair( "net", "bundle" ) );

  fd_topo_t * topo = &topology_config( 1, 1, 1, "performance" )->topo;
  static fd_topo_cpus_t cpus[1];
  fd_memset( cpus, 0, sizeof(cpus) );
  cpus->numa_node_cnt = 1UL;
  cpus->cpu_cnt       = 2UL*phys;
  for( ulong i=0UL; i<cpus->cpu_cnt; i++ ) {
    cpus->cpu[ i ] = (fd_topo_cpu_t){ .idx=i, .online=1, .sibling=i<phys ? i+phys : i-phys, .l3_idx=ULONG_MAX };
  }
  topo->blocklist_cores_cnt          = 2UL;
  topo->blocklist_cores_cpu_idx[ 0 ] = 0UL;
  topo->blocklist_cores_cpu_idx[ 1 ] = phys;
  fd_topob_auto_layout_cpus( topo, cpus );

  ulong bam    = topo->tiles[ fd_topo_find_tile( topo, "bam",    0UL ) ].cpu_idx;
  ulong bundle = topo->tiles[ fd_topo_find_tile( topo, "bundle", 0UL ) ].cpu_idx;
  FD_TEST( bam<cpus->cpu_cnt && bundle<cpus->cpu_cnt );
  if( FD_UNLIKELY( cpus->cpu[ bam ].sibling!=bundle ) )
    FD_LOG_ERR(( "%lu cores: bam on cpu %lu and bundle on cpu %lu are not siblings", phys, bam, bundle ));
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  for( ulong i=0UL; i<STUB_TILE_CNT; i++ ) {
    stub_tiles[ i ] = (fd_topo_run_tile_t){ .name=stub_tile_names[ i ], .scratch_align=stub_tile_scratch_align,
                                            .scratch_footprint=stub_tile_scratch_footprint };
    TILES[ i ] = &stub_tiles[ i ];
  }

  test_topology( 0, 0, 1, "performance" );
  test_topology( 1, 0, 1, "performance" );
  test_topology( 0, 1, 1, "performance" );
  test_topology( 1, 1, 1, "performance" );
  test_topology( 0, 1, 1, "efficient"   );
  test_topology( 1, 1, 0, "performance" );

  test_bam_smt_sibling( 24UL );
  test_bam_smt_sibling( 32UL );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
