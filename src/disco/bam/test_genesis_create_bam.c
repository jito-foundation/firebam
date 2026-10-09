/* FireBAM genesis tests.  test_genesis_create.c is included verbatim so
   these tests share its includes; its main is renamed and not run here. */

#define main test_genesis_create_upstream_main
int test_genesis_create_upstream_main( int argc, char ** argv );
#include "../../flamenco/genesis/test_genesis_create.c"
#undef main

#include "../../flamenco/runtime/program/fd_system_program.h"
#include "../bundle/fd_bundle_crank.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

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

/* With the preseeded Jito tip programs, the tip crank cranks instead of
   refusing, and is done once its update applies.  Genesis only copies
   program bytes, so stand-in files replace the ELFs. */

static void
test_bam_tip_preseed( void ) {
  static char const * const elfs[ 3 ] = {
    "spl-jito_tip_payment-0.1.10.so", "spl-jito_tip_distribution-0.1.10.so", "spl_memo-3.0.0.so" };
  char dir[] = "/tmp/test_genesis_create_bam.XXXXXX";
  FD_TEST( mkdtemp( dir ) );
  char path[ 3 ][ 128 ];
  for( ulong i=0UL; i<3UL; i++ ) {
    FD_TEST( fd_cstr_printf_check( path[ i ], sizeof(path[ i ]), NULL, "%s/%s", dir, elfs[ i ] ) );
    FILE * file = fopen( path[ i ], "wb" );
    FD_TEST( file && fputs( "ELF", file )>=0 && !fclose( file ) );
  }

  fd_genesis_options_t options[1] = {{
    .identity_pubkey             = { .ul = { 0, 0, 0, 1 } },
    .faucet_pubkey               = { .ul = { 0, 0, 0, 2 } },
    .stake_pubkey                = { .ul = { 0, 0, 0, 3 } },
    .vote_pubkey                 = { .ul = { 0, 0, 0, 4 } },
    .ticks_per_slot              = 64UL,
    .target_tick_duration_micros = 6250UL
  }};
  static uchar result_mem[ BUFSZ ];
  static uchar genesis_mem[ FD_GENESIS_FOOTPRINT( TEST_GENESIS_ACCOUNT_MAX ) ] __attribute__((aligned(FD_GENESIS_ALIGN)));
  fd_genesis_t * genesis = fd_genesis_new( genesis_mem, TEST_GENESIS_ACCOUNT_MAX );
  FD_TEST( !setenv( "FD_DEV_PRESEED_BAM_TIP_PROGRAMS_DIR", dir, 1 ) );
  ulong result_sz = fd_genesis_create( result_mem, sizeof(result_mem), options );
  FD_TEST( !unsetenv( "FD_DEV_PRESEED_BAM_TIP_PROGRAMS_DIR" ) );
  for( ulong i=0UL; i<3UL; i++ ) FD_TEST( !unlink( path[ i ] ) );
  FD_TEST( !rmdir( dir ) );
  FD_TEST( result_sz && fd_genesis_parse( genesis, result_mem, result_sz ) );

  fd_acct_addr_t tip_payment, tip_distr, builder = {{ 5 }};
  FD_TEST( fd_base58_decode_32( "T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt",  tip_payment.b ) );
  FD_TEST( fd_base58_decode_32( "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7", tip_distr.b   ) );
  static fd_bundle_crank_gen_t gen[1];
  FD_TEST( fd_bundle_crank_gen_init( gen, &tip_distr, &tip_payment, (fd_acct_addr_t const *)options->vote_pubkey.uc,
                                     &builder, "bal", 0UL ) );
  fd_acct_addr_t config_addr, tip_receiver;
  fd_bundle_crank_get_addresses( gen, 0UL, &config_addr, &tip_receiver );

  /* Every account the crank writes exists, owned by its program */
  fd_genesis_account_t account[1];
  for( ulong i=0UL; i<8UL; i++ ) {
    FD_TEST( find_account( genesis, result_mem, (fd_pubkey_t const *)gen->crank3->tip_payment_accounts[ i ], account ) );
    FD_TEST( !memcmp( account->owner.uc, tip_payment.b, 32UL ) );
  }
  FD_TEST( find_account( genesis, result_mem, (fd_pubkey_t const *)gen->crank3->tip_distribution_program_config, account ) );
  FD_TEST( !memcmp( account->owner.uc, tip_distr.b, 32UL ) );
  FD_TEST( account->data_len==88UL && FD_LOAD( ulong, account->data+72UL )==10UL && FD_LOAD( ushort, account->data+80UL )==10000 );
  FD_TEST( find_account( genesis, result_mem, (fd_pubkey_t const *)config_addr.b, account ) );
  FD_TEST( !memcmp( account->owner.uc, tip_payment.b, 32UL ) );
  FD_TEST( account->data_len>=sizeof(fd_bundle_crank_tip_payment_config_t) );
  fd_bundle_crank_tip_payment_config_t config[1];
  fd_memcpy( config, account->data, sizeof(config) );
  FD_TEST( !find_account( genesis, result_mem, (fd_pubkey_t const *)tip_receiver.b, account ) );
  fd_acct_addr_t owner = {0}; /* so the system program owns it */
  static uchar payload[ FD_TXN_MTU ];
  static uchar txn[ FD_TXN_MAX_SZ ] __attribute__((aligned(alignof(fd_txn_t))));
  fd_acct_addr_t const * identity = (fd_acct_addr_t const *)options->identity_pubkey.uc;
  FD_TEST( fd_bundle_crank_generate( gen, config, &builder, identity, &owner, 0UL, 3UL, payload, (fd_txn_t *)txn )==sizeof(fd_bundle_crank_3_t) );
  fd_bundle_crank_apply( gen, config, &builder, &owner, 0UL, 3UL );
  FD_TEST( !fd_bundle_crank_generate( gen, config, &builder, identity, &owner, 0UL, 3UL, payload, (fd_txn_t *)txn ) );
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
  test_bam_tip_preseed();

  FD_LOG_NOTICE(( "pass" ));

  fd_scratch_detach( NULL );
  fd_halt();
  return 0;
}
