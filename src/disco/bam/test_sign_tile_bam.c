/* FireBAM sign tile tests.  test_sign_tile.c is included verbatim so these
   tests share its signer fixture; its main is renamed and not run here. */

#define main test_sign_tile_upstream_main
int test_sign_tile_upstream_main( int argc, char ** argv );
#include "../keyguard/test_sign_tile.c"
#undef main

static void
test_bam_auth_request_signing( void ) {
  setup();
  ctx.in[0].role      = FD_KEYGUARD_ROLE_BAM;
  client.response_mtu = FD_ED25519_SIG_SZ;

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
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  test_bam_auth_request_signing();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
