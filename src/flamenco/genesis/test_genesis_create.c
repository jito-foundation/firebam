#include "fd_genesis_create.h"
#include "fd_genesis_parse.h"
#include "../runtime/fd_system_ids.h"
#include "../runtime/program/fd_system_program.h"
#include "../runtime/program/fd_vote_program.h"
#include "../runtime/program/vote/fd_vote_state_versioned.h"
#include "../runtime/sysvar/fd_sysvar_rent.h"
#include "../stakes/fd_stake_types.h"
#include "../../ballet/ed25519/fd_ed25519.h"
#include "../../ballet/sha256/fd_sha256.h"

#define BUFSZ (32768UL)

static void
pubkey_from_dev_seed( fd_pubkey_t * pubkey,
                      ulong         seed ) {
  uchar privkey[ 32 ] = {0};
  FD_STORE( ulong, privkey, seed );
  fd_sha512_t sha[1];
  fd_ed25519_public_from_private( pubkey->key, privkey, sha );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  /* Suppress warning logs */

  int log_level = fd_log_level_logfile();
  fd_log_level_logfile_set( fd_int_max( log_level, 4 ) );

  static uchar scratch_smem[ 16384 ];
         ulong scratch_fmem[ 4 ];
  fd_scratch_attach( scratch_smem, scratch_fmem,
                     sizeof(scratch_smem), sizeof(scratch_fmem)/sizeof(ulong) );

  /* Minimal configuration */
  fd_genesis_options_t options[1] = {{
    .identity_pubkey             = { .ul = { 0, 0, 0, 1 } },
    .faucet_pubkey               = { .ul = { 0, 0, 0, 2 } },
    .stake_pubkey                = { .ul = { 0, 0, 0, 3 } },
    .vote_pubkey                 = { .ul = { 0, 0, 0, 4 } },
    .creation_time               = 123UL,
    .ticks_per_slot              = 64UL,
    .target_tick_duration_micros = 6250UL
  }};

  /* Buffer too small */

  FD_TEST( !fd_genesis_create( NULL, 0UL, options ) );

  /* No more warnings expected */

  fd_log_level_logfile_set( log_level );

  /* Serialize to buffer */

  static uchar result_mem[ BUFSZ ];
  ulong result_sz = fd_genesis_create( result_mem, sizeof(result_mem), options );
  FD_TEST( result_sz );

  /* Now try adding a few accounts */

  options->fund_initial_accounts = 16UL;
  result_sz = fd_genesis_create( result_mem, sizeof(result_mem), options );
  FD_TEST( result_sz );

  /* Add a feature gate */
  fd_features_t features[1];
  fd_features_disable_all( features );
  options->features = features;
  result_sz = fd_genesis_create( result_mem, sizeof(result_mem), options );
  FD_TEST( result_sz );

  /* Round-trip: parse the blob back and verify the resulting genesis
     config matches the options we used to create it. */

  static fd_genesis_t genesis[1];
  FD_TEST( fd_genesis_parse( genesis, result_mem, result_sz ) );

  /* Verify POH config */

  FD_TEST( genesis->poh.ticks_per_slot == options->ticks_per_slot );
  FD_TEST( genesis->poh.tick_duration_secs == 0UL );
  FD_TEST( genesis->poh.tick_duration_ns   == options->target_tick_duration_micros * 1000UL );

  /* Verify creation time */

  FD_TEST( genesis->creation_time == options->creation_time );

  /* Verify epoch schedule defaults from fd_genesis_create */

  FD_TEST( genesis->epoch_schedule.slots_per_epoch == 8192UL );
  FD_TEST( genesis->epoch_schedule.leader_schedule_slot_offset == 8192UL );

  /* Verify rent defaults (Solana mainnet defaults) */

  FD_TEST( genesis->rent.lamports_per_uint8_year == 3480UL );
  FD_TEST( genesis->rent.burn_percent            == 50     );

  /* Verify cluster type (development) */

  FD_TEST( genesis->cluster_type == FD_GENESIS_TYPE_DEVELOPMENT );

  /* Verify account count: 4 primordial (faucet, identity, vote, stake)
     + 16 funded + some builtins.  Just check minimums. */
  FD_TEST( genesis->account_cnt >= 20UL );

  /* Verify the vote account is present with correct size */

  int found_vote = 0;
  for( ulong i=0UL; i<genesis->account_cnt; i++ ) {
    fd_genesis_account_t account[1];
    fd_genesis_account( genesis, result_mem, account, i );
    if( fd_pubkey_eq( &account->pubkey, &options->vote_pubkey ) ) {
      FD_TEST( account->data_len == FD_VOTE_STATE_V4_SZ );
      FD_TEST( !memcmp( account->owner.key, fd_solana_vote_program_id.key, 32 ) );
      FD_TEST( fd_vsv_is_correct_size_owner_and_init( account->owner.uc, account->data, account->data_len ) );
      FD_TEST( fd_vote_account_is_v4_with_bls_pubkey( account->data, account->data_len ) );
      fd_rent_t rent = {
        .lamports_per_uint8_year = genesis->rent.lamports_per_uint8_year,
        .exemption_threshold     = genesis->rent.exemption_threshold,
        .burn_percent            = genesis->rent.burn_percent
      };
      FD_TEST( account->lamports > fd_rent_exempt_minimum_balance( &rent, FD_VOTE_STATE_V4_SZ ) );

      found_vote = 1;
      break;
    }
  }
  FD_TEST( found_vote );

  /* Verify the stake account is present with correct size */

  int found_stake = 0;
  for( ulong i=0UL; i<genesis->account_cnt; i++ ) {
    fd_genesis_account_t account[1];
    fd_genesis_account( genesis, result_mem, account, i );
    if( fd_pubkey_eq( &account->pubkey, &options->stake_pubkey ) ) {
      FD_TEST( account->data_len == FD_STAKE_STATE_SZ );
      FD_TEST( !memcmp( account->owner.key, fd_solana_stake_program_id.key, 32 ) );
      FD_TEST( account->lamports > 0UL );
      found_stake = 1;
      break;
    }
  }
  FD_TEST( found_stake );

  /* Verify the optional local BAM durable nonce account. */

  FD_TEST( !setenv( "FD_DEV_PRESEED_BAM_NONCE", "1", 1 ) );
  FD_TEST( !setenv( "FD_DEV_PRESEED_BAM_NONCE_ACCOUNT_SEED", "1727000", 1 ) );
  FD_TEST( !setenv( "FD_DEV_PRESEED_BAM_NONCE_AUTH_SEED", "0", 1 ) );
  FD_TEST( !setenv( "FD_DEV_PRESEED_BAM_NONCE_HASH_SEED", "2727000", 1 ) );

  result_sz = fd_genesis_create( result_mem, sizeof(result_mem), options );
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

  /* Verify genesis hash is deterministic (same options => same hash) */

  result_sz = fd_genesis_create( result_mem, sizeof(result_mem), options );
  FD_TEST( result_sz );

  fd_hash_t hash1[1];
  fd_sha256_hash( result_mem, result_sz, hash1->hash );

  ulong result_sz2 = fd_genesis_create( result_mem, sizeof(result_mem), options );
  FD_TEST( result_sz2 == result_sz );

  fd_hash_t hash2[1];
  fd_sha256_hash( result_mem, result_sz2, hash2->hash );
  FD_TEST( fd_hash_eq( hash1, hash2 ) );

  FD_LOG_NOTICE(( "pass" ));

  fd_scratch_detach( NULL );
  fd_halt();
  return 0;
}
