#include "topology.h"
#include "config.h"
#include "../shared/fd_config_private.h"
#include "../shared/fd_action.h"
#include "../../util/pod/fd_pod_format.h"
#include "../../flamenco/accdb/fd_accdb_cache.h"
#include "../../disco/bundle/fd_bundle_crank.h"
#include "../../disco/keyguard/fd_keyguard.h"
#include "../../discof/admin/fd_admin_tile.c"

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
extern fd_topo_obj_callbacks_t fd_obj_cb_wait_info;
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
extern fd_topo_obj_callbacks_t fd_obj_cb_sleep;

fd_topo_obj_callbacks_t * CALLBACKS[] = {
  &fd_obj_cb_mcache,
  &fd_obj_cb_dcache,
  &fd_obj_cb_fseq,
  &fd_obj_cb_metrics,
  &fd_obj_cb_netdev_tbl,
  &fd_obj_cb_neigh4_hmap,
  &fd_obj_cb_keyswitch,
  &fd_obj_cb_node_info,
  &fd_obj_cb_wait_info,
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
  &fd_obj_cb_sleep,
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
STUB_TILE( motor   );
STUB_TILE( rotor   );
STUB_TILE( votor   );
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
STUB_TILE( waker   );
STUB_TILE( mwaitx  );

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
  &stub_tile_motor,
  &stub_tile_rotor,
  &stub_tile_votor,
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
  &stub_tile_waker,
  &stub_tile_mwaitx,
  NULL,
};

#undef STUB_TILE

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
  static char const * const halt[] = { "replay", "repair", "rotor", "gossip", "tower", "votor", "txsend", "bundle", "rserve", "bam" };
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
  ulong voter_idx = fd_topo_find_tile( topo, "tower", 0UL );
  admin->alpenglow = voter_idx==ULONG_MAX;
  if( admin->alpenglow ) voter_idx = fd_topo_find_tile( topo, "votor", 0UL );
  FD_TEST( voter_idx!=ULONG_MAX );
  admin->voter_name = topo->tiles[ voter_idx ].name;

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
    done = poll_set_identity( admin, &state, 1UL, keypair );
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
   and bundle claims the publishing bit, both in bam_status.  A writable
   mapping elsewhere (e.g. gossip, which parses untrusted network input)
   could flip the override bit or scramble the ownership generation. */

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
                    ( !strcmp( wksp, "bam_status" ) && ( !strcmp( tile->name, "pack" ) || !strcmp( tile->name, "bundle" ) ) );
        if( FD_UNLIKELY( !owner ) ) FD_LOG_ERR(( "%s:%lu maps BAM control workspace %s writable", tile->name, tile->kind_id, wksp ));
      }
    }
  }
}

/* The contact refresh and bandwidth streams are optional GUI outputs.
   Enabling BAM together with GUI must retain their producers and polling
   modes while gossip still accepts BAM's ownership handoff. */
static void
test_gossip_gui_links( fd_topo_t const * topo,
                       int               gui_enabled,
                       int               leader_enabled,
                       int               rpc_enabled,
                       int               alpenglow_enabled ) {
  ulong gossip_id = fd_topo_find_tile( topo, "gossip", 0UL );
  FD_TEST( gossip_id!=ULONG_MAX );
  fd_topo_tile_t const * gossip = &topo->tiles[ gossip_id ];
  FD_TEST( fd_topo_find_link( topo, "gossip_out", 0UL )==ULONG_MAX );
  static char const * const required_outs[] = { "gossip_ciaddr", "gossip_misc" };
  for( ulong i=0UL; i<sizeof(required_outs)/sizeof(required_outs[0]); i++ )
    FD_TEST( fd_topo_find_tile_out_link( topo, gossip, required_outs[ i ], 0UL )!=ULONG_MAX );

  /* RPC needs unverified legacy gossip votes even without a leader
     pipeline.  Alpenglow RPC obtains certificates through votor_out. */
  int votes_enabled = leader_enabled || (rpc_enabled && !alpenglow_enabled);
  ulong vote_link = fd_topo_find_link( topo, "gossip_vote", 0UL );
  FD_TEST( (vote_link!=ULONG_MAX)==votes_enabled );
  FD_TEST( (fd_topo_find_tile_out_link( topo, gossip, "gossip_vote", 0UL )!=ULONG_MAX)==votes_enabled );
  if( votes_enabled )
    FD_TEST( fd_topo_link_consumer_cnt( topo, &topo->links[ vote_link ] )==
             fd_topo_tile_name_cnt( topo, "verify" )+(ulong)(rpc_enabled && !alpenglow_enabled) );
  for( ulong i=0UL; i<fd_topo_tile_name_cnt( topo, "verify" ); i++ ) {
    fd_topo_tile_t const * verify = &topo->tiles[ fd_topo_find_tile( topo, "verify", i ) ];
    ulong in_idx = fd_topo_find_tile_in_link( topo, verify, "gossip_vote", 0UL );
    FD_TEST( in_idx!=ULONG_MAX && verify->in_link_poll[ in_idx ] && verify->in_link_reliable[ in_idx ] );
  }
  ulong rpc_id = fd_topo_find_tile( topo, "rpc", 0UL );
  FD_TEST( (rpc_id!=ULONG_MAX)==rpc_enabled );
  if( rpc_enabled ) {
    fd_topo_tile_t const * rpc = &topo->tiles[ rpc_id ];
    ulong in_idx = fd_topo_find_tile_in_link( topo, rpc, "gossip_ciaddr", 0UL );
    FD_TEST( in_idx!=ULONG_MAX && rpc->in_link_poll[ in_idx ] && rpc->in_link_reliable[ in_idx ] );
    FD_TEST( (fd_topo_find_tile_in_link( topo, rpc, "gossip_vote", 0UL )!=ULONG_MAX)==!alpenglow_enabled );
  }

  ulong gui_id = fd_topo_find_tile( topo, "gui", 0UL );
  FD_TEST( (gui_id!=ULONG_MAX)==gui_enabled );
  FD_TEST( (fd_topo_find_link( topo, "gossip_ciseen", 0UL )!=ULONG_MAX)==gui_enabled );
  FD_TEST( (fd_topo_find_link( topo, "gossip_gui", 0UL )!=ULONG_MAX)==gui_enabled );
  if( !gui_enabled ) return;
  fd_topo_tile_t const * gui = &topo->tiles[ gui_id ];
  for( ulong i=0UL; i<gui->in_cnt; i++ ) FD_TEST( gui->in_link_poll[ i ] );

  static char const * const gui_outs[] = { "gossip_ciseen", "gossip_gui" };
  for( ulong i=0UL; i<sizeof(gui_outs)/sizeof(gui_outs[0]); i++ ) {
    ulong link_id = fd_topo_find_link( topo, gui_outs[ i ], 0UL );
    ulong out_idx = fd_topo_find_tile_out_link( topo, gossip, gui_outs[ i ], 0UL );
    ulong in_idx  = fd_topo_find_tile_in_link( topo, gui, gui_outs[ i ], 0UL );
    FD_TEST( link_id!=ULONG_MAX && out_idx!=ULONG_MAX && in_idx!=ULONG_MAX );
    FD_TEST( gossip->out_link_id[ out_idx ]==link_id && gui->in_link_id[ in_idx ]==link_id );
    FD_TEST( !!gui->in_link_reliable[ in_idx ]==(i==0UL) );
  }
  for( ulong i=0UL; i<fd_topo_tile_name_cnt( topo, "gossvf" ); i++ ) {
    ulong tile_id = fd_topo_find_tile( topo, "gossvf", i );
    fd_topo_tile_t const * gossvf = &topo->tiles[ tile_id ];
    ulong link_id = fd_topo_find_link( topo, "gossvf_gui", i );
    ulong out_idx = fd_topo_find_tile_out_link( topo, gossvf, "gossvf_gui", i );
    ulong in_idx  = fd_topo_find_tile_in_link( topo, gui, "gossvf_gui", i );
    FD_TEST( link_id!=ULONG_MAX && out_idx!=ULONG_MAX && in_idx!=ULONG_MAX );
    FD_TEST( gossvf->out_link_id[ out_idx ]==link_id && gui->in_link_id[ in_idx ]==link_id );
    FD_TEST( !gui->in_link_reliable[ in_idx ] );
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
  fd_acct_addr_t identity;
  uchar private_key[32] = {1,2,3};
  fd_sha512_t sha[1];
  FD_TEST( fd_sha512_join( fd_sha512_new( sha ) ) );
  fd_ed25519_public_from_private( identity.b, private_key, sha );

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
    fd_keyguard_authority_t bad = authority;
    bad.identity_pubkey[0] ^= 1U;
    FD_TEST( !fd_keyguard_payload_authorize( &bad, message, message_sz,
                                             FD_KEYGUARD_ROLE_BUNDLE_CRANK, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
    bad = authority;
    bad.tip_payment_program[0] ^= 1U;
    FD_TEST( !fd_keyguard_payload_authorize( &bad, message, message_sz,
                                             FD_KEYGUARD_ROLE_BUNDLE_CRANK, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
    if( create ) {
      /* Only the create-account crank invokes the distribution program. */
      bad = authority;
      bad.tip_distribution_program[0] ^= 1U;
      FD_TEST( !fd_keyguard_payload_authorize( &bad, message, message_sz,
                                               FD_KEYGUARD_ROLE_BUNDLE_CRANK, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
    }
    uchar * signature = (uchar *)fd_txn_get_signatures( txn, payload );
    fd_ed25519_sign( signature, message, message_sz, identity.b, private_key, sha );
    FD_TEST( fd_ed25519_verify( message, message_sz, signature, identity.b, sha )==FD_ED25519_SUCCESS );
  }
}

/* Exercise every BAM/bundle mode: missing shared crank/sign wiring breaks
   BAM-only config and identity updates, while an unwanted source tile leaves
   a disabled ingress path active. */
static void
test_topology( int          bundle_enabled,
               int          bam_enabled,
               int          alpenglow_enabled,
               int          gui_enabled,
               int          leader_enabled,
               int          rpc_enabled,
               char const * layout_mode ) {
  static config_t config[1];
  fd_memset( config, 0, sizeof(config_t) );
  config->is_firedancer = 1;
  fd_config_load_buf( config, (char const *)firedancer_default_config,
                     firedancer_default_config_sz, "default.toml" );
  fd_config_validate( config );
  /* fd_config_load_buf parses TOML only.  Normal startup computes this
     derived limit after validation, before building the topology. */
  config->limits.max_txn_per_slot = FD_MAX_TXN_PER_SLOT;

  fd_cstr_ncpy( config->net.provider, "socket", sizeof(config->net.provider) );
  config->telemetry = 0;
  config->tiles.gui.enabled = gui_enabled;
  config->tiles.bundle.enabled = bundle_enabled;
  config->tiles.bam.enabled = bam_enabled;
  config->tiles.rpc.enabled = rpc_enabled;
  config->firedancer.layout.enable_block_production = leader_enabled;
  config->firedancer.development.alpenglow = alpenglow_enabled;

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

  int crank_enabled = leader_enabled && (bundle_enabled || bam_enabled);
  fd_topo_t const * topo = &config->topo;
  test_gossip_gui_links( topo, gui_enabled, leader_enabled, rpc_enabled, alpenglow_enabled );
  /* Deduplication must retain one access declaration per object even
     when several links share a dcache or a tile also owns its input. */
  for( ulong i=0UL; i<topo->tile_cnt; i++ )
    for( ulong j=0UL; j<topo->tiles[ i ].uses_obj_cnt; j++ )
      for( ulong k=0UL; k<j; k++ )
        FD_TEST( topo->tiles[ i ].uses_obj_id[ j ]!=topo->tiles[ i ].uses_obj_id[ k ] );
  FD_TEST( (fd_topo_find_tile( topo, "txsend", 0UL )!=ULONG_MAX)==!alpenglow_enabled );
  FD_TEST( (fd_topo_find_link( topo, "txsend_out", 0UL )!=ULONG_MAX)==!alpenglow_enabled );
  FD_TEST( (fd_topo_find_link( topo, "txsend_sign", 0UL )!=ULONG_MAX)==!alpenglow_enabled );
  FD_TEST( (fd_topo_find_link( topo, "sign_txsend", 0UL )!=ULONG_MAX)==!alpenglow_enabled );

  if( !leader_enabled ) {
    static char const * const leader_tiles[] = { "quic", "verify", "dedup", "resolv", "pack", "execle", "poh", "motor", "bundle", "bam" };
    static char const * const leader_links[] = { "pack_sign", "sign_pack", "bam_verif", "bam_sign", "bam_gossip", "bam_shred", "executed_txn", "pack_bam_ldr", "pack_bam_res", "bank_bam", "poh_bam" };
    for( ulong i=0UL; i<sizeof(leader_tiles)/sizeof(leader_tiles[0]); i++ )
      FD_TEST( fd_topo_find_tile( topo, leader_tiles[ i ], 0UL )==ULONG_MAX );
    for( ulong i=0UL; i<sizeof(leader_links)/sizeof(leader_links[0]); i++ )
      FD_TEST( fd_topo_find_link( topo, leader_links[ i ], 0UL )==ULONG_MAX );
    fd_topo_tile_t const * replay = &topo->tiles[ fd_topo_find_tile( topo, "replay", 0UL ) ];
    FD_TEST( !replay->replay.bundle.enabled );
    FD_TEST( !fd_topo_find_tile_obj( topo, replay, "bam_ctrl" ) );
    FD_TEST( fd_pod_query_ulong( topo->props, "bam_status", ULONG_MAX )==ULONG_MAX );
    FD_TEST( fd_pod_query_ulong( topo->props, "bam_gen", ULONG_MAX )==ULONG_MAX );
    fd_topo_tile_t const * sign = &topo->tiles[ fd_topo_find_tile( topo, "sign", 0UL ) ];
    uchar disabled_program[32];
    fd_memset( disabled_program, 0xFF, sizeof(disabled_program) );
    FD_TEST( fd_memeq( sign->sign.bundle.tip_distribution_program_addr, disabled_program, 32UL ) );
    FD_TEST( fd_memeq( sign->sign.bundle.tip_payment_program_addr, disabled_program, 32UL ) );
    fd_topo_obj_t const * accdb = fd_topo_find_obj( topo, "accdb", NULL, ULONG_MAX );
    FD_TEST( accdb );
    FD_TEST( fd_pod_queryf_ulong( topo->props, 0UL, "obj.%lu.cache_min_reserved", accdb->id )==fd_accdb_cache_min_reserved( 0 ) );
    test_set_identity( &config->topo );
    return;
  }

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
  FD_TEST( sign->id_keyswitch_obj_id!=ULONG_MAX );
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
  FD_TEST( (fd_topo_find_tile_obj( topo, &topo->tiles[ replay_id ], "bam_ctrl" )!=NULL)==bam_enabled );

  FD_TEST( (fd_topo_find_tile( topo, "bundle", 0UL )!=ULONG_MAX)==bundle_enabled );
  FD_TEST( (fd_topo_find_tile( topo, "bam",    0UL )!=ULONG_MAX)==bam_enabled );
  if( bam_enabled ) {
    fd_topo_tile_t const * bam = &topo->tiles[ fd_topo_find_tile( topo, "bam", 0UL ) ];
    /* BAM consumes terminal worker results as one contiguous polled
       block, followed immediately by the PoH result input. */
    ulong worker_cnt = fd_topo_tile_name_cnt( topo, "execle" );
    ulong bank_in = fd_topo_find_tile_in_link( topo, bam, "bank_bam", 0UL );
    FD_TEST( worker_cnt && bank_in!=ULONG_MAX );
    for( ulong i=0UL; i<worker_cnt; i++ ) {
      ulong in = fd_topo_find_tile_in_link( topo, bam, "bank_bam", i );
      FD_TEST( in==bank_in+i && bam->in_link_poll[ in ] && bam->in_link_reliable[ in ] );
      fd_topo_tile_t const * worker = &topo->tiles[ fd_topo_find_tile( topo, "execle", i ) ];
      ulong out = fd_topo_find_tile_out_link( topo, worker, "bank_bam", i );
      FD_TEST( out!=ULONG_MAX && worker->out_link_id[ out ]==bam->in_link_id[ in ] );
    }
    ulong poh_in = fd_topo_find_tile_in_link( topo, bam, "poh_bam", 0UL );
    FD_TEST( poh_in==bank_in+worker_cnt && bam->in_link_poll[ poh_in ] && bam->in_link_reliable[ poh_in ] );
    ulong result_cnt = 0UL;
    for( ulong i=0UL; i<bam->in_cnt; i++ ) {
      char const * name = topo->links[ bam->in_link_id[ i ] ].name;
      result_cnt += !strcmp( name, "bank_bam" ) || !strcmp( name, "poh_bam" );
    }
    FD_TEST( result_cnt==worker_cnt+1UL );
    ulong sign_in = fd_topo_find_tile_in_link( topo, bam, "sign_bam", 0UL );
    FD_TEST( sign_in>poh_in && sign_in!=ULONG_MAX && !bam->in_link_poll[ sign_in ] );
    ulong replay_in = fd_topo_find_tile_in_link( topo, bam, "replay_slot", 0UL );
    FD_TEST( replay_in!=ULONG_MAX );
    FD_TEST( bam->in_link_poll[ replay_in ] && !bam->in_link_reliable[ replay_in ] );
    FD_TEST( fd_topo_find_tile_in_link( topo, bam, "replay_out", 0UL )==ULONG_MAX );
    /* Only efficient layout parks tiles; performance layout polls the
       BAM socket instead of waiting for the waker tile. */
    FD_TEST( bam->id_keyswitch_obj_id!=ULONG_MAX );
    FD_TEST( bam->is_waker_client==!strcmp( layout_mode, "efficient" ) );
    FD_TEST( (bam->waker_client_idx!=ULONG_MAX)==bam->is_waker_client );
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
     per-transaction publish. */
  FD_TEST( (fd_topo_find_link( topo, "executed_txn", 0UL )!=ULONG_MAX)==bam_enabled );
  fd_topo_tile_t const * pack_tile  = &topo->tiles[ fd_topo_find_tile( topo, "pack",  0UL ) ];
  fd_topo_tile_t const * dedup_tile = &topo->tiles[ fd_topo_find_tile( topo, "dedup", 0UL ) ];
  FD_TEST( (fd_topo_find_tile_in_link( topo, pack_tile,  "executed_txn", 0UL )!=ULONG_MAX)==bam_enabled );
  FD_TEST( (fd_topo_find_tile_in_link( topo, dedup_tile, "executed_txn", 0UL )!=ULONG_MAX)==bam_enabled );

  ulong poh_id = fd_topo_find_tile( topo, alpenglow_enabled ? "motor" : "poh", 0UL );
  FD_TEST( poh_id!=ULONG_MAX );
  FD_TEST( (fd_topo_find_tile_out_link( topo, &topo->tiles[ poh_id ], "executed_txn", 0UL )!=ULONG_MAX)==bam_enabled );
  FD_TEST( (fd_topo_find_tile_out_link( topo, &topo->tiles[ poh_id ], "poh_bam", 0UL )!=ULONG_MAX)==bam_enabled );

  fd_topo_obj_t const * accdb = fd_topo_find_obj( topo, "accdb", NULL, ULONG_MAX );
  FD_TEST( accdb );
  FD_TEST( fd_pod_queryf_ulong( topo->props, 0UL, "obj.%lu.cache_min_reserved", accdb->id )==
           fd_accdb_cache_min_reserved( crank_enabled ) );

  test_set_identity( &config->topo );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  char const * const layouts[] = { "performance", "efficient" };
  for( int alpenglow=0; alpenglow<=1; alpenglow++ )
    for( ulong layout=0UL; layout<sizeof(layouts)/sizeof(layouts[0]); layout++ )
      for( int gui=0; gui<=1; gui++ )
        for( int leader=0; leader<=1; leader++ )
          for( int rpc=0; rpc<=1; rpc++ )
            for( int bundle=0; bundle<=1; bundle++ )
              for( int bam=0; bam<=1; bam++ )
                test_topology( bundle, bam, alpenglow, gui, leader, rpc, layouts[ layout ] );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
