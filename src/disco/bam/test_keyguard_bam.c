/* FireBAM keyguard and sign tile tests.  test_sign_tile.c is included
   verbatim so these tests share its signer fixture; its main is renamed
   and not run here. */

#define main test_sign_tile_upstream_main
int test_sign_tile_upstream_main( int argc, char ** argv );
#include "../keyguard/test_sign_tile.c"
#undef main

#include "../../flamenco/gossip/fd_gossip_message.h"

static void
test_bam_auth_authorize( void ) {
  fd_keyguard_authority_t authority = {0};
  uchar payload[25];
  memcpy( payload, "X_OFF_CHAIN_JITO_BAM_V1\0", 24UL );
  payload[24] = 'x';

  FD_TEST( fd_keyguard_payload_match( payload, sizeof(payload), FD_KEYGUARD_SIGN_TYPE_ED25519 )==FD_KEYGUARD_PAYLOAD_BAM_AUTH );
  FD_TEST( !fd_keyguard_payload_match( payload, sizeof(payload), FD_KEYGUARD_SIGN_TYPE_BLS_PUBKEY ) );
  FD_TEST( fd_keyguard_payload_authorize( &authority, payload, sizeof(payload), FD_KEYGUARD_ROLE_BAM, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
  FD_TEST( !fd_keyguard_payload_authorize( &authority, payload, sizeof(payload), FD_KEYGUARD_ROLE_VOTOR, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
  FD_TEST( !fd_keyguard_payload_authorize( &authority, payload, sizeof(payload), FD_KEYGUARD_ROLE_TOWER, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
  FD_TEST( !fd_keyguard_payload_authorize( &authority, payload, sizeof(payload), FD_KEYGUARD_ROLE_BAM, FD_KEYGUARD_SIGN_TYPE_BLS ) );
  FD_TEST( !fd_keyguard_payload_authorize( &authority, payload, sizeof(payload), FD_KEYGUARD_ROLE_BAM, FD_KEYGUARD_SIGN_TYPE_BLS_PUBKEY ) );

  /* A long challenge also reaches the minimum vote-history body size.
     Keep BAM matching and authorization distinct across the full range. */
  uchar long_payload[ 24UL+128UL+1UL ];
  memcpy( long_payload, payload, 24UL );
  memset( long_payload+24UL, 'x', sizeof(long_payload)-24UL );
  FD_TEST( !fd_keyguard_payload_match( long_payload, 24UL, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
  FD_TEST( fd_keyguard_payload_match( long_payload, 112UL, FD_KEYGUARD_SIGN_TYPE_ED25519 )==FD_KEYGUARD_PAYLOAD_BAM_AUTH );
  FD_TEST( fd_keyguard_payload_match( long_payload, 152UL, FD_KEYGUARD_SIGN_TYPE_ED25519 )==FD_KEYGUARD_PAYLOAD_BAM_AUTH );
  FD_TEST( fd_keyguard_payload_authorize( &authority, long_payload, 152UL, FD_KEYGUARD_ROLE_BAM, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
  FD_TEST( !fd_keyguard_payload_match( long_payload, sizeof(long_payload), FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
  long_payload[ 0 ] ^= (uchar)1;
  FD_TEST( !fd_keyguard_payload_match( long_payload, 152UL, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
  long_payload[ 0 ] ^= (uchar)1;
  long_payload[ 23 ] = (uchar)'x'; /* the label's NUL */
  FD_TEST( !( fd_keyguard_payload_match( long_payload, 152UL, FD_KEYGUARD_SIGN_TYPE_ED25519 ) & FD_KEYGUARD_PAYLOAD_BAM_AUTH ) );

  /* Binary challenge bytes can read as empty vote-history collections.
     The exact-mask authorization must reject the ambiguous request. */
  uchar ambiguous_payload[ 112UL ] = {0};
  memcpy( ambiguous_payload, payload, 24UL );
  FD_TEST( fd_keyguard_payload_match( ambiguous_payload, sizeof(ambiguous_payload), FD_KEYGUARD_SIGN_TYPE_ED25519 )==
           (FD_KEYGUARD_PAYLOAD_BAM_AUTH|FD_KEYGUARD_PAYLOAD_VOTE_HISTORY) );
  FD_TEST( !fd_keyguard_payload_authorize( &authority, ambiguous_payload, sizeof(ambiguous_payload),
                                          FD_KEYGUARD_ROLE_BAM, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );

  /* An eight-byte challenge also matches the unrestricted 32-byte
     Merkle shred fingerprint.  Both roles reject this ambiguity. */
  uchar shred_payload[ 32UL ];
  memcpy( shred_payload, payload, 24UL );
  memset( shred_payload+24UL, 'x', sizeof(shred_payload)-24UL );
  FD_TEST( fd_keyguard_payload_match( shred_payload, sizeof(shred_payload), FD_KEYGUARD_SIGN_TYPE_ED25519 )==
           (FD_KEYGUARD_PAYLOAD_BAM_AUTH|FD_KEYGUARD_PAYLOAD_SHRED) );
  FD_TEST( !fd_keyguard_payload_authorize( &authority, shred_payload, sizeof(shred_payload),
                                          FD_KEYGUARD_ROLE_BAM, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
  FD_TEST( !fd_keyguard_payload_authorize( &authority, shred_payload, sizeof(shred_payload),
                                          FD_KEYGUARD_ROLE_LEADER, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );

  uchar query[ sizeof(ulong)+1UL ] = {0}; /* authority index */
  FD_TEST( !fd_keyguard_payload_authorize( &authority, query, sizeof(ulong), FD_KEYGUARD_ROLE_BAM, FD_KEYGUARD_SIGN_TYPE_BLS_PUBKEY ) );

  uchar skip[11] = { 3 /* Alpenglow skip vote */ };
  FD_TEST( fd_keyguard_payload_match( skip, sizeof(skip), FD_KEYGUARD_SIGN_TYPE_BLS )==FD_KEYGUARD_PAYLOAD_AG_VOTE );
  FD_TEST( !fd_keyguard_payload_authorize( &authority, skip, sizeof(skip), FD_KEYGUARD_ROLE_BAM, FD_KEYGUARD_SIGN_TYPE_BLS ) );
  FD_TEST( fd_keyguard_payload_authorize( &authority, skip, sizeof(skip), FD_KEYGUARD_ROLE_VOTOR, FD_KEYGUARD_SIGN_TYPE_BLS ) );

  /* The BAM role never signs the smallest vote history body (identity,
     nine empty collections and root) or the smallest tower body. */
  ulong const vote_history_sz = 32UL + 9UL*8UL + 8UL;
  ulong const tower_sz        = 48UL + (65UL+8UL+12UL+1UL+8UL+32UL*48UL+8UL+1UL+8UL+16UL) + (4UL+74UL+2UL) + 16UL;
  static uchar body[ FD_KEYGUARD_SIGN_REQ_MTU ];
  memset( &authority, 0xAA, sizeof(authority) );
  memcpy( body, authority.identity_pubkey, 32UL );
  FD_TEST( fd_keyguard_payload_match( body, vote_history_sz, FD_KEYGUARD_SIGN_TYPE_ED25519 )==FD_KEYGUARD_PAYLOAD_VOTE_HISTORY );
  FD_TEST( !fd_keyguard_payload_authorize( &authority, body, vote_history_sz, FD_KEYGUARD_ROLE_BAM, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
  FD_STORE( ulong,  body+32UL,  8UL     ); /* threshold_depth */
  FD_STORE( double, body+40UL,  2.0/3.0 ); /* threshold_size */
  FD_STORE( ulong,  body+113UL, 1UL     ); /* votes_cnt */
  FD_TEST( fd_keyguard_payload_match( body, tower_sz, FD_KEYGUARD_SIGN_TYPE_ED25519 )==FD_KEYGUARD_PAYLOAD_TOWER );
  FD_TEST( !fd_keyguard_payload_authorize( &authority, body, tower_sz, FD_KEYGUARD_ROLE_BAM, FD_KEYGUARD_SIGN_TYPE_ED25519 ) );
}

static int
authorize_contact_info_client_id( ushort client_id ) {
  fd_keyguard_authority_t authority = {0};

  fd_gossip_value_t value;
  fd_memset( &value, 0, sizeof(value) );

  for( uchar i=0U; i<32U; i++ ) {
    authority.identity_pubkey[ i ] = i;
    value.origin[ i ]              = i;
  }

  value.tag = FD_GOSSIP_VALUE_CONTACT_INFO;
  value.wallclock = 0UL;
  value.contact_info->outset              = 1UL;
  value.contact_info->shred_version       = 1U;
  value.contact_info->version.feature_set = FD_FEATURE_SET_ID;
  value.contact_info->version.client      = client_id;

  uchar crds_value[ FD_GOSSIP_VALUE_MAX_SZ ];
  long crds_value_sz = fd_gossip_value_serialize( &value, crds_value, sizeof(crds_value) );
  FD_TEST( crds_value_sz>64L );

  return fd_keyguard_payload_authorize( &authority,
                                        crds_value+64UL,
                                        (ulong)(crds_value_sz-64L),
                                        FD_KEYGUARD_ROLE_GOSSIP,
                                        FD_KEYGUARD_SIGN_TYPE_ED25519 );
}

/* The tile addresses link data from the workspace holding the dcache,
   and frag chunks are 32 bits, so each dcache is in a workspace. */
static void
test_sign_link( fd_topo_link_t * link,
                fd_wksp_t *      wksp,
                char const *     name,
                ulong            mtu ) {
  ulong  data_sz = fd_dcache_req_data_sz( mtu, TEST_DEPTH, 1UL, 1 );
  void * mem     = fd_wksp_alloc_laddr( wksp, fd_dcache_align(), fd_dcache_footprint( data_sz, 0UL ), 1UL );
  fd_cstr_ncpy( link->name, name, sizeof(link->name) );
  link->mtu    = mtu;
  link->dcache = fd_dcache_join( fd_dcache_new( mem, data_sz, 0UL ) );
  FD_TEST( link->dcache );
}

/* The sign tile's link setup gives only a bam_sign input, answered on
   sign_bam, the BAM role.  The tile maps both links into ctx, and the
   BAM auth payload is signed over them. */
static void
test_bam_auth_request_signing( void ) {
  static fd_topo_t      topo[1];
  static fd_keyswitch_t keyswitch[1];
  setup();

  /* A sign tile with one in and one out.  Its objects sit at their
     offsets in a NULL-based workspace. */
  fd_wksp_t * wksp = fd_wksp_new_anonymous( FD_SHMEM_NORMAL_PAGE_SZ, 64UL, fd_shmem_cpu_idx( 0UL ), "sign-bam-test", 16UL );
  FD_TEST( wksp );
  fd_memset( topo, 0, sizeof(fd_topo_t) );
  topo->objs[ 0 ] = (fd_topo_obj_t){ .id=0UL, .offset=(ulong)&ctx };
  topo->objs[ 1 ] = (fd_topo_obj_t){ .id=1UL, .offset=(ulong)fd_keyswitch_new( keyswitch, FD_KEYSWITCH_STATE_UNLOCKED ) };
  test_sign_link( &topo->links[ 0 ], wksp, "bam_sign", 256UL );
  test_sign_link( &topo->links[ 1 ], wksp, "sign_bam",  64UL );
  fd_topo_tile_t * tile = &topo->tiles[ 0 ];
  tile->tile_obj_id         = 0UL;
  tile->id_keyswitch_obj_id = 1UL;
  tile->av_keyswitch_obj_id = ULONG_MAX;
  tile->in_cnt              = 1UL;
  tile->in_link_id[ 0 ]     = 0UL;
  tile->out_cnt             = 1UL;
  tile->out_link_id[ 0 ]    = 1UL;

  unprivileged_init( topo, tile );
  FD_TEST( ctx.in[0].role==FD_KEYGUARD_ROLE_BAM );
  FD_TEST( ctx.in[0].mem==wksp && ctx.out[0].out_mem==wksp );

  client.request_mem     = ctx.in[0].mem;
  client.request_chunk0  = ctx.in[0].chunk0;
  client.request_chunk   = ctx.in[0].chunk0;
  client.request_wmark   = ctx.in[0].wmark;
  client.request_mtu     = ctx.in[0].mtu;
  client.response_mem    = ctx.out[0].out_mem;
  client.response_chunk0 = ctx.out[0].out_chunk0;
  client.response_wmark  = ctx.out[0].out_wmark;
  client.response_mtu    = FD_ED25519_SIG_SZ;

  uchar payload[25];
  memcpy( payload, "X_OFF_CHAIN_JITO_BAM_V1\0", 24UL );
  payload[24] = 'x';
  uchar signature[ FD_ED25519_SIG_SZ ];

  ulong request_cnt = 1UL;
  pthread_t signer;
  FD_TEST( !pthread_create( &signer, NULL, sign_requests, &request_cnt ) );
  fd_keyguard_client_sign( &client, signature, payload, sizeof(payload), FD_KEYGUARD_SIGN_TYPE_ED25519 );
  FD_TEST( !pthread_join( signer, NULL ) );
  fd_frag_meta_t const * response = client.response+fd_mcache_line_idx( 0UL, TEST_DEPTH );
  FD_TEST( response->sz==FD_ED25519_SIG_SZ );
  FD_TEST( !fd_ed25519_verify( payload, sizeof(payload), signature, ctx.public_key, ctx.sha512 ) );

  for( ulong i=0UL; i<2UL; i++ ) fd_wksp_free_laddr( fd_dcache_delete( fd_dcache_leave( topo->links[ i ].dcache ) ) );
  fd_wksp_delete_anonymous( wksp );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  /* Calibrate the tick rate in ~1 ms instead of ~0.5 s. */
  fd_tempo_tick_per_ns_dev( NULL );
  test_bam_auth_authorize();
  FD_TEST( authorize_contact_info_client_id( FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER ) );
  FD_TEST( authorize_contact_info_client_id( FD_GOSSIP_CONTACT_INFO_CLIENT_FIRE_BAM   ) );
  FD_TEST( !authorize_contact_info_client_id( FD_GOSSIP_CONTACT_INFO_CLIENT_AGAVE_BAM ) );
  test_bam_auth_request_signing();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
