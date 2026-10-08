/* FireBAM keyguard tests.  test_keyguard.c is included verbatim so these
   tests share its includes; its main is renamed and not run here. */

#define main test_keyguard_upstream_main
int test_keyguard_upstream_main( int argc, char ** argv );
#include "../keyguard/test_keyguard.c"
#undef main

static void
test_bam_auth_authorize( void ) {
  fd_keyguard_authority_t authority = {0};
  uchar payload[25];
  memcpy( payload, "X_OFF_CHAIN_JITO_BAM_V1\0", 24UL );
  payload[24] = 'x';

  FD_TEST( FD_KEYGUARD_ROLE_VOTOR==9 && FD_KEYGUARD_ROLE_TOWER==10 && FD_KEYGUARD_ROLE_BAM==11 );
  FD_TEST( FD_KEYGUARD_PAYLOAD_BLS_PUBKEY==(1UL<<12) );
  FD_TEST( FD_KEYGUARD_PAYLOAD_TOWER     ==(1UL<<13) );
  FD_TEST( FD_KEYGUARD_PAYLOAD_VOTE_HISTORY==(1UL<<14) );
  FD_TEST( FD_KEYGUARD_PAYLOAD_BAM_AUTH  ==(1UL<<15) );
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

int
main( int     argc,
      char ** argv ) {
  fd_log_private_boot( &argc, &argv );
  test_bam_auth_authorize();
  FD_LOG_NOTICE(( "pass" ));
  return 0;
}
