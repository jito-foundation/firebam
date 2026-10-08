/* FireBAM genesis tests.  test_genesis_create.c is included verbatim so
   these tests share its includes; its main is renamed and not run here. */

#define main test_genesis_create_upstream_main
int test_genesis_create_upstream_main( int argc, char ** argv );
#include "../../flamenco/genesis/test_genesis_create.c"
#undef main

#include "../../flamenco/runtime/program/fd_system_program.h"
#include <stdlib.h>

static void
pubkey_from_dev_seed( fd_pubkey_t * pubkey,
                      ulong         seed ) {
  uchar privkey[ 32 ] = {0};
  FD_STORE( ulong, privkey, seed );
  fd_sha512_t sha[1];
  fd_ed25519_public_from_private( pubkey->key, privkey, sha );
}

/* Verify the optional local BAM durable nonce account. */

static void
test_bam_nonce_preseed( void ) {
  fd_features_t features[1];
  fd_features_disable_all( features );
  fd_genesis_options_t options[1] = {{
    .identity_pubkey             = { .ul = { 0, 0, 0, 1 } },
    .faucet_pubkey               = { .ul = { 0, 0, 0, 2 } },
    .stake_pubkey                = { .ul = { 0, 0, 0, 3 } },
    .vote_pubkey                 = { .ul = { 0, 0, 0, 4 } },
    .creation_time               = 123UL,
    .ticks_per_slot              = 64UL,
    .target_tick_duration_micros = 6250UL,
    .fund_initial_accounts       = 16UL,
    .features                    = features
  }};

  static uchar result_mem[ BUFSZ ];
  static uchar genesis_mem[ FD_GENESIS_FOOTPRINT( TEST_GENESIS_ACCOUNT_MAX ) ] __attribute__((aligned(FD_GENESIS_ALIGN)));
  fd_genesis_t * genesis = fd_genesis_new( genesis_mem, TEST_GENESIS_ACCOUNT_MAX );
  FD_TEST( genesis );

  FD_TEST( !setenv( "FD_DEV_PRESEED_BAM_NONCE", "1", 1 ) );
  FD_TEST( !setenv( "FD_DEV_PRESEED_BAM_NONCE_ACCOUNT_SEED", "1727000", 1 ) );
  FD_TEST( !setenv( "FD_DEV_PRESEED_BAM_NONCE_AUTH_SEED", "0", 1 ) );
  FD_TEST( !setenv( "FD_DEV_PRESEED_BAM_NONCE_HASH_SEED", "2727000", 1 ) );

  ulong result_sz = fd_genesis_create( result_mem, sizeof(result_mem), options );
  FD_TEST( result_sz );
  FD_TEST( fd_genesis_parse( genesis, result_mem, result_sz ) );

  fd_pubkey_t expected_nonce_account[1];
  fd_pubkey_t expected_nonce_auth[1];
  fd_pubkey_t expected_nonce_hash_key[1];
  pubkey_from_dev_seed( expected_nonce_account, 1727000UL );
  pubkey_from_dev_seed( expected_nonce_auth,          0UL );
  pubkey_from_dev_seed( expected_nonce_hash_key, 2727000UL );

  int found_nonce = 0;
  for( ulong i=0UL; i<genesis->account_cnt; i++ ) {
    fd_genesis_account_t account[1];
    fd_genesis_account( genesis, result_mem, account, i );
    if( fd_pubkey_eq( &account->pubkey, expected_nonce_account ) ) {
      FD_TEST( account->data_len==FD_SYSTEM_PROGRAM_NONCE_DLEN );
      FD_TEST( !memcmp( account->owner.key, fd_solana_system_program_id.key, 32UL ) );
      FD_TEST( account->lamports>0UL );

      fd_nonce_state_versions_t state[1];
      FD_TEST( !fd_nonce_state_versions_decode( state, account->data, account->data_len ) );
      FD_TEST( state->version==FD_NONCE_VERSION_CURRENT );
      FD_TEST( state->kind==FD_NONCE_STATE_INITIALIZED );
      FD_TEST( fd_pubkey_eq( &state->authority, expected_nonce_auth ) );
      FD_TEST( !memcmp( state->durable_nonce.hash,
                        expected_nonce_hash_key->key,
                        sizeof(fd_hash_t) ) );
      FD_TEST( state->lamports_per_signature==
               genesis->fee_rate_governor.target_lamports_per_signature );
      found_nonce = 1;
      break;
    }
  }
  FD_TEST( found_nonce );

  unsetenv( "FD_DEV_PRESEED_BAM_NONCE" );
  unsetenv( "FD_DEV_PRESEED_BAM_NONCE_ACCOUNT_SEED" );
  unsetenv( "FD_DEV_PRESEED_BAM_NONCE_AUTH_SEED" );
  unsetenv( "FD_DEV_PRESEED_BAM_NONCE_HASH_SEED" );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  static uchar scratch_smem[ 65536 ];
         ulong scratch_fmem[ 4 ];
  fd_scratch_attach( scratch_smem, scratch_fmem,
                     sizeof(scratch_smem), sizeof(scratch_fmem)/sizeof(ulong) );

  test_bam_nonce_preseed();

  FD_LOG_NOTICE(( "pass" ));

  fd_scratch_detach( NULL );
  fd_halt();
  return 0;
}
