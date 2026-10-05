/* Reward admission's runtime boundary: use the real fork-aware accounts
   DB and runtime finalizer. The upstream replay harness supplies unused
   transport/execution mocks; parenthesized calls below invoke real runtime
   symbols. Votor aggregates are trusted verified ingress, as in a leader
   bank; this test does not replace Votor's BLS verification tests. */
#define main test_replay_tile_main
#include "../../discof/replay/test_replay_tile.c"
#undef main
#include "../../flamenco/runtime/tests/fd_svm_mini.h"
#include "../../flamenco/runtime/program/vote/fd_vote_codec_tmpl.h"
#include "../../flamenco/runtime/fd_pubkey_utils.h"
#include "../../flamenco/runtime/fd_system_ids.h"
#include "../../ballet/hex/fd_hex.h"

struct reward_snapshot {
  fd_block_footer_t footer;
  fd_hash_t hash;
  fd_lthash_value_t lthash;
  ulong credits[2];
  ulong last_vote[2];
  ulong lamports[2];
  ulong data_sz[2];
  uchar data[2][FD_VOTE_STATE_V4_SZ];
};

static void
put_reward_pda( fd_svm_mini_t * mini, char const * seed, uchar * data, ulong data_sz ) {
  fd_pubkey_t program = ids[offsetof(fd_features_t, alpenglow)>>3].id;
  uchar const * seeds[1] = { (uchar const *)seed };
  ulong sizes[1] = { strlen(seed) };
  fd_pubkey_t address;
  uchar bump;
  uint err = 0U;
  FD_TEST( !fd_pubkey_find_program_address( &program, 1UL, seeds, sizes, &address, &bump, &err ) );
  fd_acc_t acc = { .lamports=1000000UL, .data_len=data_sz, .data=data };
  fd_memcpy( acc.pubkey, address.uc, 32UL );
  fd_memcpy( acc.owner, fd_solana_system_program_id.uc, 32UL );
  fd_svm_mini_put_account_rooted( mini, &acc );
}

static void
runtime_reward_case( fd_svm_mini_t * mini, int delayed, int omit, struct reward_snapshot * out ) {
  fd_svm_mini_params_t params[1];
  fd_svm_mini_params_default( params );
  params->mock_validator_cnt = 2UL;
  ulong root_idx = fd_svm_mini_reset( mini, params );
  fd_pubkey_t feature = ids[offsetof(fd_features_t, alpenglow)>>3].id;
  uchar feature_data[9] = {1};
  fd_acc_t feature_acc = { .lamports=1000000UL, .data_len=sizeof(feature_data), .data=feature_data };
  fd_memcpy( feature_acc.pubkey, feature.uc, 32UL );
  fd_memcpy( feature_acc.owner, fd_solana_feature_program_id.uc, 32UL );
  fd_svm_mini_put_account_rooted( mini, &feature_acc );
  uchar migration[8]; FD_STORE( ulong, migration, 1UL );
  put_reward_pda( mini, "carlgration", migration, sizeof(migration) );
  uchar parent_time[8] = {0};
  put_reward_pda( mini, "alpenclock", parent_time, sizeof(parent_time) );
  uchar inflation[25] = {0};
  FD_STORE( ulong, inflation, 1000000UL );
  FD_STORE( ulong, inflation+8UL, 16UL );
  put_reward_pda( mini, "vote_reward_account", inflation, sizeof(inflation) );

  fd_pubkey_t identity[2], vote[2];
  fd_rng_t rng[1]; fd_rng_join( fd_rng_new( rng, (uint)params->hash_seed, 0UL ) );
  for( ulong i=0UL; i<2UL; i++ ) {
    for( ulong j=0UL; j<4UL; j++ ) identity[i].ul[j] = fd_rng_ulong( rng );
    for( ulong j=0UL; j<4UL; j++ ) vote[i].ul[j] = fd_rng_ulong( rng );
    for( ulong j=0UL; j<4UL; j++ ) (void)fd_rng_ulong( rng );
  }
  fd_rng_delete( fd_rng_leave( rng ) );
  ulong bank_idx = fd_svm_mini_attach_child( mini, root_idx, 10UL );
  fd_bank_t * bank = fd_svm_mini_bank( mini, bank_idx );
  bank->is_leader = 1; /* Votor is the BLS verification boundary for leaders. */
  FD_FEATURE_SET_ACTIVE( &bank->f.features, alpenglow, 0UL );
  fd_vote_stakes_t * stakes = fd_bank_vote_stakes( bank );
  fd_vote_stakes_reset( stakes );
  bank->vote_stakes_fork_id = fd_vote_stakes_init( stakes, bank->f.epoch );
  uchar bls[2][FD_BLS_PUBKEY_COMPRESSED_SZ];
  fd_hex_decode( bls[0], "97f1d3a73197d7942695638c4fa9ac0fc3688c4f9774b905a14e3a3f171bac586c55e83ff97a1aeffb3af00adb22c6bb", sizeof(bls[0]) );
  fd_hex_decode( bls[1], "af9ff5448e60bc9a718f463ac102bd6f8772e6460c19076a6c89d5806e5a8ef44b6f3b8af09e37a4e564987a26b9deda", sizeof(bls[1]) );
  for( ulong i=0UL; i<2UL; i++ )
    fd_vote_stakes_snap_insert_t_2( stakes, bank->vote_stakes_fork_id, &vote[i], &identity[i], 200UL-i*100UL, 0U, bls[i] );
  fd_vote_stakes_finalize( stakes, bank->vote_stakes_fork_id, FD_VOTE_STAKES_ITER_T_2 );

  static fd_replay_tile_t ctx[1];
  fd_memset( ctx, 0, sizeof(ctx) );
  ctx->banks = mini->banks;
  ctx->next_leader_slot = 10UL;
  ctx->votor_window_start_slot = 10UL;
  ctx->votor_leader_seq = 10UL;
  ctx->votor_leader_valid = 1;
  ctx->votor_final->slot = ULONG_MAX;
  ctx->replay_out->idx = ULONG_MAX; /* No synthetic bank creation. */
  fd_votor_msg_t * message = fd_wksp_alloc_laddr( mini->wksp, FD_CHUNK_ALIGN, sizeof(fd_votor_msg_t), 42UL );
  FD_TEST( message );
  ulong chunk = fd_laddr_to_chunk( mini->wksp, message );
  ctx->in_kind[0] = IN_KIND_VOTOR;
  ctx->in[0].mem = mini->wksp;
  ctx->in[0].chunk0 = ctx->in[0].wmark = chunk;
  ctx->in[0].mtu = sizeof(fd_votor_msg_t);
  fd_votor_reward_t * reward = (fd_votor_reward_t *)message;
  *reward = (fd_votor_reward_t){ .slot=2UL };
  fd_bls_agg_null( &reward->agg_notar );
  fd_bls_agg_null( &reward->agg_skip );
  fd_bls_set_insert( reward->agg_skip.set, 0UL );
  FD_TEST( !returnable_frag( ctx, 0UL, 9UL, FD_VOTOR_SIG_REWARD, chunk, sizeof(*message), 0UL, 0UL, 0UL, NULL ) );
  FD_TEST( !replay_reward_ready( ctx ) ); /* Same slot, before LEADER, is insufficient. */
  if( delayed ) {
    for( ulong i=0UL; i<1000UL; i++ ) FD_TEST( !replay_reward_ready( ctx ) );
    FD_TEST( !ctx->leader_reward_window.valid );
  }
  FD_TEST( !returnable_frag( ctx, 0UL, 11UL, FD_VOTOR_SIG_REWARD, chunk, sizeof(*message), 0UL, 0UL, 0UL, NULL ) );
  FD_TEST( replay_reward_ready( ctx ) );
  replay_pin_reward_window( ctx );
  ctx->leader_bank = bank;
  fd_memset( out, 0, sizeof(*out) );
  out->footer.block_producer_time_nanos = 1000000000UL;
  construct_footer_certs( ctx, 10UL, fd_alpenglow_migration_slot( bank, mini->runtime->accdb ), &out->footer );
  FD_TEST( out->footer.has_skip_reward_cert && out->footer.skip_reward_cert.slot==2UL );
  if( omit ) out->footer.has_skip_reward_cert = 0;
  /* The mini executor requires a unique completed blockhash, like its
     ordinary freeze helper. Real motor hashing is covered separately. */
  fd_sha256_hash( bank->f.poh.hash, 32UL, bank->f.poh.hash );
  FD_TEST( !(fd_runtime_block_execute_finalize)( bank, mini->runtime->accdb, NULL, &out->footer, 0U ) );
  out->hash = bank->f.bank_hash;
  out->lthash = bank->f.lthash;
  for( ulong i=0UL; i<2UL; i++ ) {
    fd_acc_t acc = fd_accdb_read_one( mini->runtime->accdb, bank->accdb_fork_id, vote[i].uc );
    FD_TEST( acc.lamports && acc.data_len<=sizeof(out->data[i]) );
    out->lamports[i] = acc.lamports;
    out->data_sz[i] = acc.data_len;
    fd_memcpy( out->data[i], acc.data, acc.data_len );
    fd_vote_state_versioned_t state[1];
    FD_TEST( !fd_vsv_deserialize( &acc, state ) );
    ulong const * last = fd_vsv_get_last_voted_slot( state );
    out->last_vote[i] = last ? *last : ULONG_MAX;
    fd_vote_epoch_credits_t const * credits = fd_vsv_get_epoch_credits( state );
    if( !deq_fd_vote_epoch_credits_t_empty( credits ) )
      out->credits[i] = deq_fd_vote_epoch_credits_t_peek_tail_const( credits )->credits;
    fd_accdb_unread_one( mini->runtime->accdb, &acc );
  }
  fd_wksp_free_laddr( message );
}

int
main( int argc, char ** argv ) {
  fd_svm_mini_limits_t limits[1]; fd_svm_mini_limits_default( limits );
  fd_svm_mini_t * mini = fd_svm_test_boot( &argc, &argv, limits );
  static struct reward_snapshot immediate, delayed, omitted;
  runtime_reward_case( mini, 0, 0, &immediate );
  runtime_reward_case( mini, 1, 0, &delayed );
  runtime_reward_case( mini, 1, 1, &omitted );
  FD_TEST( !memcmp( &immediate, &delayed, sizeof(immediate) ) );
  FD_TEST( immediate.last_vote[0]==2UL && immediate.last_vote[1]!=2UL );
  FD_TEST( omitted.last_vote[0]!=2UL );
  FD_TEST( immediate.credits[0]>omitted.credits[0] );
  FD_TEST( memcmp( &immediate.hash, &omitted.hash, sizeof(fd_hash_t) ) );
  FD_TEST( memcmp( &immediate.lthash, &omitted.lthash, sizeof(fd_lthash_value_t) ) );
  FD_TEST( memcmp( immediate.data[0], omitted.data[0], immediate.data_sz[0] ) );
  FD_LOG_NOTICE(( "pass: real reward settlement preserves certificates, accounts, credits, lthash and bank hash across delayed receipt; omission changes state" ));
  fd_svm_test_halt( mini );
  return 0;
}
