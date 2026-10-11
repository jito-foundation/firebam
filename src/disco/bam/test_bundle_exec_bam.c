/* FireBAM bundle execution tests.  test_bundle_exec.c is included
   verbatim so these tests share its fixtures; its main is renamed and
   not run here. */

#define main test_bundle_exec_upstream_main
int test_bundle_exec_upstream_main( int argc, char ** argv );
#include "../../flamenco/runtime/test_bundle_exec.c"
#undef main

#include "../../flamenco/runtime/fd_runtime_const.h"
#include "../../flamenco/runtime/sysvar/fd_sysvar_recent_hashes.h"

#define TEST_BAM_FILLER_MAX (8UL)

/* env contains fd_txn_out_t which has a 10MB nonce_rollback_data
   buffer — must not be on the stack. */
static test_env_t test_bam_env[1];

/* test_bam_reset_world mirrors test_execute_bundles' reset_world. */
static void
test_bam_reset_world( test_env_t *    env,
                      fd_svm_mini_t * mini ) {
  fd_pubkey_t system  = {0};
  fd_pubkey_t pubkey1 = { .ul[0] = 1UL };
  fd_pubkey_t pubkey2 = { .ul[0] = 2UL };
  fd_pubkey_t pubkey3 = { .ul[0] = 3UL };
  uchar data2[5] = {6, 7, 8, 9, 10};
  uchar data3[5] = {11, 12, 13, 14, 15};
  setup_env( env, mini );
  create_test_account( env->mini->runtime->accdb, env->fork_id, &pubkey1, 1000000UL, 0UL, NULL,  &system );
  create_test_account( env->mini->runtime->accdb, env->fork_id, &pubkey2, 1000000UL, 5UL, data2, &system );
  create_test_account( env->mini->runtime->accdb, env->fork_id, &pubkey3, 1000000UL, 5UL, data3, &system );
  for( ulong i=0UL; i<TEST_BAM_FILLER_MAX; i++ ) {
    fd_pubkey_t filler = { .ul[0] = 0xF000UL + i };
    create_test_account( env->mini->runtime->accdb, env->fork_id, &filler, 1000000UL, 0UL, NULL, &system );
  }
}

/* A parent-visible ALT address receives the writable permission from a
   later bundle transaction's resolved key, even though tx0 does not
   reference it. */
static void
test_bundle_alt_parent_writable( fd_svm_mini_t * mini ) {
  test_env_t * env = test_bam_env;
  test_bam_reset_world( env, mini );

  fd_pubkey_t    pubkey1   = { .ul[0] = 1UL };
  fd_signature_t signature = {0};
  fd_hash_t      dummy_hash = {0};
  fd_memset( dummy_hash.uc, 0xAB, FD_HASH_FOOTPRINT );
  fd_txn_p_t     txn_p = {0};
  ulong          sz;

  /* Initialize slot hashes sysvar so the sysvar cache can serve them
     to fd_executor_setup_txn_alut_account_keys. */

  uchar slot_hashes_data[ FD_SYSVAR_SLOT_HASHES_BINCODE_SZ ];
  ulong sh_cnt = 10UL;
  FD_STORE( ulong, slot_hashes_data, sh_cnt );
  fd_slot_hash_t * sh_entries = (fd_slot_hash_t *)( slot_hashes_data + sizeof(ulong) );
  for( ulong i = 0UL; i < sh_cnt; i++ ) {
    sh_entries[i].slot = 10UL - i;
    fd_memset( sh_entries[i].hash.hash, 0, 32UL );
  }
  ulong sh_sz = sizeof(ulong) + sh_cnt * sizeof(fd_slot_hash_t);
  fd_sysvar_account_update( env->bank, env->runtime->accdb, NULL, &fd_sysvar_slot_hashes_id, slot_hashes_data, sh_sz );

  fd_sysvar_cache_restore( env->bank, env->mini->runtime->accdb );

  fd_pubkey_t alut_key = { .ul[0] = 0xA107UL };
  ulong num_alut_addrs = 4UL;
  ulong alut_data_sz   = FD_LOOKUP_TABLE_META_SIZE + num_alut_addrs * 32UL;
  uchar alut_data[ FD_LOOKUP_TABLE_META_SIZE + 4 * 32 ];

  fd_alut_meta_t alut_meta = {
    .discriminant                   = FD_ALUT_STATE_DISC_LOOKUP_TABLE,
    .deactivation_slot              = ULONG_MAX,
    .last_extended_slot             = 10UL,
    .last_extended_slot_start_index = 2,
    .has_authority                  = 0,
  };
  FD_TEST( fd_alut_state_encode( &alut_meta, alut_data, FD_LOOKUP_TABLE_META_SIZE ) == 0 );

  fd_acct_addr_t * alut_addrs = (fd_acct_addr_t *)( alut_data + FD_LOOKUP_TABLE_META_SIZE );
  for( ulong i = 0UL; i < num_alut_addrs; i++ ) {
    fd_memset( alut_addrs[i].b, 0, 32UL );
    alut_addrs[i].b[0] = (uchar)( 0xE0 + i );
    alut_addrs[i].b[1] = (uchar)( 0xF0 + i );
  }

  create_test_account( env->mini->runtime->accdb, env->bank->parent_accdb_fork_id, &alut_key, 1000000UL,
                       (uint)alut_data_sz, alut_data,
                       &fd_solana_address_lookup_table_program_id );

  /* tx0 references the ALT account but not the address tx1 resolves. */
  fd_pubkey_t alt_resolved_addr = {0};
  alt_resolved_addr.uc[0] = 0xE3;
  alt_resolved_addr.uc[1] = 0xF3;
  fd_pubkey_t txn0_keys[3] = { pubkey1, alut_key, alt_resolved_addr };
  txn_p = (fd_txn_p_t){0};
  sz = txn_serialize( txn_p.payload, 1, &signature, 1UL, 0UL, 1UL,
                      3UL, txn0_keys, &dummy_hash );
  txn_p.payload_sz = (ushort)sz;
  FD_TEST( fd_txn_parse( txn_p.payload, sz, TXN( &txn_p ), NULL ) );

  /* tx1: V0 transaction resolving parent-visible ALT index 1 writable. */
  fd_txn_p_t txn1_p = {0};
  uchar * pl  = txn1_p.payload;
  ulong   off = 0UL;

  pl[off++] = 1;
  fd_memset( pl + off, 0, 64UL );
  off += 64UL;

  pl[off++] = 0x80;
  pl[off++] = 1;
  pl[off++] = 0;
  pl[off++] = 0;

  pl[off++] = 1;
  fd_memcpy( pl + off, &pubkey1, 32UL );
  ulong acct_addr_off = off;
  off += 32UL;

  ulong rbh_off = off;
  fd_memcpy( pl + off, &dummy_hash, 32UL );
  off += 32UL;

  pl[off++] = 0;

  pl[off++] = 1;
  ulong alut_addr_payload_off = off;
  fd_memcpy( pl + off, &alut_key, 32UL );
  off += 32UL;
  pl[off++] = 1;
  pl[off++] = 1;
  ulong writable_off = off - 1UL;
  pl[off++] = 0;

  txn1_p.payload_sz = (ushort)off;

  uchar txn1_mem[ sizeof(fd_txn_t) + sizeof(fd_txn_acct_addr_lut_t) ] __attribute__((aligned(16UL)));
  fd_txn_t * txn1 = (fd_txn_t *)txn1_mem;
  fd_memset( txn1, 0, sizeof(txn1_mem) );

  txn1->transaction_version          = FD_TXN_V0;
  txn1->signature_cnt                = 1;
  txn1->signature_off                = 1;
  txn1->message_off                  = 65;
  txn1->readonly_signed_cnt          = 0;
  txn1->readonly_unsigned_cnt        = 0;
  txn1->acct_addr_cnt                = 1;
  txn1->acct_addr_off                = (ushort)acct_addr_off;
  txn1->recent_blockhash_off         = (ushort)rbh_off;
  txn1->instr_cnt                    = 0;
  txn1->addr_table_lookup_cnt        = 1;
  txn1->addr_table_adtl_writable_cnt = 1;
  txn1->addr_table_adtl_cnt          = 1;

  fd_txn_acct_addr_lut_t * lut = fd_txn_get_address_tables( txn1 );
  lut->addr_off     = (ushort)alut_addr_payload_off;
  lut->writable_cnt = 1;
  lut->writable_off = (ushort)writable_off;
  lut->readonly_cnt = 0;
  lut->readonly_off = (ushort)off;

  FD_TEST( sizeof(txn1_mem) <= sizeof(txn1_p._) );
  fd_memcpy( txn1_p._, txn1_mem, sizeof(txn1_mem) );

  fd_txn_in_t alut_bundle_in[2] = {0};
  alut_bundle_in[0].txn              = &txn_p;
  alut_bundle_in[0].bundle.is_bundle = 1;
  alut_bundle_in[1].txn              = &txn1_p;
  alut_bundle_in[1].bundle.is_bundle = 1;

  int prep_err = fd_runtime_prepare_bundle_accounts( env->runtime, env->bank, alut_bundle_in, env->txn_out, 2UL );
  FD_TEST( prep_err==FD_RUNTIME_EXECUTE_SUCCESS );
  int saw_writable_alt = 0;
  for( ulong i=0UL; i<env->runtime->accounts.account_cnt; i++ ) {
    fd_acc_t const * acc = &env->runtime->accounts.account[i];
    if( FD_LIKELY( memcmp( acc->pubkey, alut_addrs[1].b, 32UL ) ) ) continue;
    FD_TEST( acc->_writable );
    saw_writable_alt = 1;
  }
  FD_TEST( saw_writable_alt );
  fd_runtime_fini_bundle( env->runtime );

  FD_LOG_NOTICE(( "test bundle ALT parent-visible writable... ok" ));
}

static void
test_self_authorized_nonce( fd_svm_mini_t * mini ) {
  test_env_t * env = test_bam_env;
  fd_signature_t signature = {0};
  ulong          sz;

  /* A self-authorized AdvanceNonce can use two instruction accounts: the
     nonce account (also signer/fee payer) and RecentBlockhashes.  With
     only the nonce account, age checks still pass, but execution fails
     because the RecentBlockhashes instruction account is missing. */
  for( ulong instr_acct_cnt=2UL; instr_acct_cnt>=1UL; instr_acct_cnt-- ) {
    test_bam_reset_world( env, mini );
    fd_pubkey_t nonce_key = { .ul[0] = 0x5E1FA17UL };
    fd_hash_t old_blockhash = {0};
    fd_memset( old_blockhash.uc, 0x11, FD_HASH_FOOTPRINT );
    fd_hash_t old_nonce;
    durable_nonce_from_blockhash( &old_nonce, &old_blockhash );
    FD_TEST( !fd_blockhashes_check_age( &env->bank->f.block_hash_queue, &old_nonce,
                                        FD_SYSVAR_RECENT_HASHES_CAP ) );
    create_test_account( env->mini->runtime->accdb, env->fork_id, &fd_solana_system_program_id, 1UL,
                         0UL, NULL, &fd_solana_native_loader_id );
    create_nonce_account_initialized( env, &nonce_key, &nonce_key, &old_nonce );

    fd_pubkey_t nonce_keys[3] = {
      nonce_key, fd_sysvar_recent_block_hashes_id, fd_solana_system_program_id
    };
    if( instr_acct_cnt==1UL ) nonce_keys[1] = fd_solana_system_program_id;
    uchar ix_accts[2] = { 0, 1 };
    uchar ix_data[4];
    FD_STORE( uint, ix_data, (uint)FD_SYSTEM_PROGRAM_INSTR_ADVANCE_NONCE_ACCOUNT );
    txn_instr_t nonce_instr = {
      .program_id_idx   = (uchar)instr_acct_cnt,
      .account_idxs     = ix_accts,
      .account_idxs_cnt = (ushort)instr_acct_cnt,
      .data             = ix_data,
      .data_sz          = sizeof(ix_data)
    };
    fd_txn_p_t nonce_txn = {0};
    sz = txn_serialize_with_instrs( nonce_txn.payload, 1, &signature,
                                    1UL, 0UL, instr_acct_cnt, instr_acct_cnt+1UL, nonce_keys,
                                    &old_nonce, &nonce_instr, 1U );
    nonce_txn.payload_sz = (ushort)sz;
    FD_TEST( fd_txn_parse( nonce_txn.payload, sz, TXN( &nonce_txn ), NULL ) );

    env->txn_in.txn              = &nonce_txn;
    env->txn_in.bundle.is_bundle = 0;
    fd_runtime_prepare_and_execute_txn( env->runtime, env->bank, &env->txn_in, &env->txn_out[0] );
    FD_TEST( env->txn_out[0].err.is_committable );
    FD_TEST( env->txn_out[0].err.txn_err==(instr_acct_cnt==2UL
                                          ? FD_RUNTIME_EXECUTE_SUCCESS
                                          : FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR) );
    fd_runtime_commit_txn( env->runtime, env->bank, NULL, &env->txn_out[0] );

    fd_acc_t acc = fd_accdb_read_one( env->mini->runtime->accdb, env->fork_id, nonce_key.key );
    fd_nonce_state_versions_t state[1];
    FD_TEST( !fd_nonce_state_versions_decode( state, acc.data, acc.data_len ) );
    fd_hash_t const * latest_blockhash = fd_blockhashes_peek_last_hash( &env->bank->f.block_hash_queue );
    FD_TEST( latest_blockhash );
    fd_hash_t expected_nonce;
    durable_nonce_from_blockhash( &expected_nonce, latest_blockhash );
    FD_TEST( fd_pubkey_eq( &state->authority, &nonce_key ) );
    FD_TEST( !memcmp( &state->durable_nonce, &expected_nonce, sizeof(fd_hash_t) ) );
    FD_TEST( memcmp( &state->durable_nonce, &old_nonce, sizeof(fd_hash_t) ) );
    fd_accdb_unread_one( env->mini->runtime->accdb, &acc );
    FD_LOG_NOTICE(( "test self-authorized %lu-account nonce... ok", instr_acct_cnt ));
  }
}

static void
test_fresh_hash_unsigned_advance_nonce( fd_svm_mini_t * mini ) {
  test_env_t * env = test_bam_env;
  fd_signature_t signature = {0};
  fd_hash_t      dummy_hash = {0};
  fd_memset( dummy_hash.uc, 0xAB, FD_HASH_FOOTPRINT );
  fd_pubkey_t    filler[1] = { { .ul[0] = 0xF000UL } };
  ulong          sz;

  /* A nonce-shaped instruction with no instruction-account signer is not
     a durable nonce, but its fresh blockhash lets the ordinary bank age
     path accept it.  The instruction fails and still pays its fee. */
  {
    test_bam_reset_world( env, mini );
    fd_pubkey_t fee_payer = { .ul[0] = 0xFEE2UL };
    fd_pubkey_t nonce_key = { .ul[0] = 0x5E1FA18UL };
    fd_hash_t old_blockhash = {0};
    fd_memset( old_blockhash.uc, 0x11, FD_HASH_FOOTPRINT );
    fd_hash_t old_nonce;
    durable_nonce_from_blockhash( &old_nonce, &old_blockhash );
    FD_TEST( fd_blockhashes_check_age( &env->bank->f.block_hash_queue, &dummy_hash,
                                       FD_SYSVAR_RECENT_HASHES_CAP ) );
    create_test_account( env->mini->runtime->accdb, env->fork_id, &fee_payer, 10000000UL,
                         0UL, NULL, &fd_solana_system_program_id );
    create_test_account( env->mini->runtime->accdb, env->fork_id, &fd_solana_system_program_id, 1UL,
                         0UL, NULL, &fd_solana_native_loader_id );
    create_nonce_account_initialized( env, &nonce_key, &fee_payer, &old_nonce );

    fd_pubkey_t nonce_keys[5] = {
      fee_payer, nonce_key, fd_sysvar_recent_block_hashes_id, filler[0], fd_solana_system_program_id
    };
    uchar ix_accts[3] = { 1, 2, 3 };
    uchar ix_data[4];
    FD_STORE( uint, ix_data, (uint)FD_SYSTEM_PROGRAM_INSTR_ADVANCE_NONCE_ACCOUNT );
    txn_instr_t nonce_instr = {
      .program_id_idx   = 4,
      .account_idxs     = ix_accts,
      .account_idxs_cnt = 3,
      .data             = ix_data,
      .data_sz          = sizeof(ix_data)
    };
    fd_txn_p_t nonce_txn = {0};
    sz = txn_serialize_with_instrs( nonce_txn.payload, 1, &signature,
                                    1UL, 0UL, 3UL, 5UL, nonce_keys,
                                    &dummy_hash, &nonce_instr, 1U );
    nonce_txn.payload_sz = (ushort)sz;
    FD_TEST( fd_txn_parse( nonce_txn.payload, sz, TXN( &nonce_txn ), NULL ) );
    for( ulong i=0UL; i<3UL; i++ ) FD_TEST( !fd_txn_is_signer( TXN( &nonce_txn ), ix_accts[i] ) );

    env->txn_in.txn              = &nonce_txn;
    env->txn_in.bundle.is_bundle = 0;
    fd_runtime_prepare_and_execute_txn( env->runtime, env->bank, &env->txn_in, &env->txn_out[0] );
    FD_TEST( env->txn_out[0].err.is_committable );
    FD_TEST( env->txn_out[0].err.txn_err==FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR );
    fd_runtime_commit_txn( env->runtime, env->bank, NULL, &env->txn_out[0] );

    fd_acc_t payer = fd_accdb_read_one( env->mini->runtime->accdb, env->fork_id, fee_payer.key );
    FD_TEST( payer.lamports==10000000UL-FD_RUNTIME_FEE_STRUCTURE_LAMPORTS_PER_SIGNATURE );
    fd_accdb_unread_one( env->mini->runtime->accdb, &payer );
    fd_acc_t nonce = fd_accdb_read_one( env->mini->runtime->accdb, env->fork_id, nonce_key.key );
    fd_nonce_state_versions_t state[1];
    FD_TEST( !fd_nonce_state_versions_decode( state, nonce.data, nonce.data_len ) );
    FD_TEST( !memcmp( &state->durable_nonce, &old_nonce, sizeof(fd_hash_t) ) );
    fd_accdb_unread_one( env->mini->runtime->accdb, &nonce );
    FD_LOG_NOTICE(( "test fresh-hash unsigned AdvanceNonce pays fee... ok" ));
  }
}

int
main( int     argc,
      char ** argv ) {
  fd_svm_mini_limits_t limits[1];
  fd_svm_mini_limits_default( limits );
  fd_svm_mini_t * mini = fd_svm_test_boot( &argc, &argv, limits );

  test_bundle_alt_parent_writable( mini );
  test_self_authorized_nonce( mini );
  test_fresh_hash_unsigned_advance_nonce( mini );

  FD_LOG_NOTICE(( "pass" ));
  fd_svm_test_halt( mini );
  return 0;
}
