/* test_execle_tile unit tests leader transaction execution by mocking an
   execle tile context and using fd_svm_mini for runtime state.

   The execle tile is mostly the same as the execrp tile, except for
   bundle execution. */

#define _GNU_SOURCE
#include "fd_execle_tile.c"
#include "../../ballet/txn/fd_txn_build.h"
#include "../../disco/topo/fd_topob.h"
#include "../../flamenco/accdb/fd_accdb.h"
#include "../../flamenco/runtime/tests/fd_svm_mini.h"
#include "../../flamenco/runtime/fd_system_ids.h"
#include "../../flamenco/runtime/fd_system_ids_pp.h"
#include "../../flamenco/runtime/program/fd_system_program.h"
#include "../../flamenco/runtime/program/fd_bpf_loader_program.h"
#include "../../flamenco/runtime/program/vote/fd_vote_codec.h"
#include "../../ballet/txn/fd_compact_u16.h"
#include "../../disco/pack/fd_pack.h"
#include "test_bam_poh_fixture.h"
#include "../../util/tmpl/fd_unit_test.c"
#include <unistd.h>

#define MAX_LIVE_SLOTS   32
#define MAX_TXN_PER_SLOT 32

#define TOPO_TAG 2UL

int volatile const fd_startup_skip_checks = 1; /* fd_startup.c */

static fd_svm_mini_t * mini;
static fd_topo_t       topo[1];
static uchar           metrics_scratch[ FD_METRICS_FOOTPRINT( 0UL ) ] __attribute__((aligned(FD_METRICS_ALIGN)));

FD_IMPORT_BINARY( test_bpf_program, "src/ballet/sbpf/fixtures/hello_solana_program.so" );

struct test_env {
  void *             tile_mem;
  fd_svm_mini_t *    mini;
  fd_execle_tile_t * execle;
  ulong              bank_idx;
};

typedef struct test_env test_env_t;

static void
test_mock_validator_keys( fd_pubkey_t * identity_key,
                          fd_pubkey_t * vote_key ) {
  fd_rng_t rng[1];
  FD_TEST( fd_rng_join( fd_rng_new( rng, 1U, 0UL ) ) );

  for( ulong j=0UL; j<4UL; j++ ) identity_key->ul[j] = fd_rng_ulong( rng );
  for( ulong j=0UL; j<4UL; j++ ) vote_key->ul[j]     = fd_rng_ulong( rng );
  for( ulong j=0UL; j<4UL; j++ ) (void)fd_rng_ulong( rng );

  fd_rng_delete( fd_rng_leave( rng ) );
}

static fd_topo_obj_t *
test_topo_obj_laddr( fd_topo_t *  topo,
                     char const * obj_type,
                     char const * wksp_name,
                     void *       laddr ) {
  fd_topo_obj_t * obj = fd_topob_obj( topo, obj_type, wksp_name );
  obj->offset = (ulong)fd_wksp_gaddr_fast( topo->workspaces[ obj->wksp_id ].wksp, laddr );
  return obj;
}

static void
test_topo_link_init( test_env_t *     env,
                     fd_topo_t *      topo,
                     fd_topo_link_t * link ) {
  ulong mcache_footprint = fd_mcache_footprint( link->depth, 0UL );
  void * mcache_mem = fd_wksp_alloc_laddr( env->mini->wksp, fd_mcache_align(), mcache_footprint, TOPO_TAG );
  FD_TEST( fd_mcache_new( mcache_mem, link->depth, 0UL, 0UL ) );
  link->mcache = fd_mcache_join( mcache_mem );
  FD_TEST( link->mcache );
  topo->objs[ link->mcache_obj_id ].offset = fd_wksp_gaddr_fast( env->mini->wksp, mcache_mem );

  if( link->mtu ) {
    ulong data_sz = fd_dcache_req_data_sz( link->mtu, link->depth, link->burst, 1 );
    ulong dcache_footprint = fd_dcache_footprint( data_sz, 0UL );
    void * dcache_mem = fd_wksp_alloc_laddr( env->mini->wksp, fd_dcache_align(), dcache_footprint, TOPO_TAG );
    FD_TEST( fd_dcache_new( dcache_mem, data_sz, 0UL ) );
    link->dcache = fd_dcache_join( dcache_mem );
    FD_TEST( link->dcache );
    topo->objs[ link->dcache_obj_id ].offset = fd_wksp_gaddr_fast( env->mini->wksp, dcache_mem );
  }
}

static fd_topo_link_t *
test_topo_link_kind( char const * name,
                     ulong        kind_id ) {
  for( ulong i=0UL; i<topo->link_cnt; i++ ) {
    if( !strcmp( topo->links[i].name, name ) && topo->links[i].kind_id==kind_id ) return &topo->links[i];
  }
  FD_LOG_ERR(( "missing test topo link %s", name ));
}

static fd_topo_link_t *
test_topo_link( char const * name ) {
  return test_topo_link_kind( name, 0UL );
}

static test_env_t *
test_env_create_worker( test_env_t const * sibling ) {
  test_env_t * env = fd_wksp_alloc_laddr( mini->wksp, alignof(test_env_t), sizeof(test_env_t), TOPO_TAG );
  FD_TEST( env );
  memset( env, 0, sizeof(test_env_t) );

  env->mini = mini;

  if( sibling ) env->bank_idx = sibling->bank_idx;
  else {
    fd_svm_mini_params_t params[1];
    fd_svm_mini_params_default( params );
    ulong root_idx = fd_svm_mini_reset( env->mini, params );
    env->bank_idx = fd_svm_mini_attach_child( env->mini, root_idx, 2UL );

    fd_topob_new( topo, "execle" );
    fd_topo_wksp_t * topo_wksp = fd_topob_wksp( topo, "execle" );
    topo_wksp->wksp = env->mini->wksp;
  }
  fd_topo_tile_t * topo_tile = fd_topob_tile( topo, "execle", "execle", "execle", 0UL, 0, 0, 0 );
  ulong kind_id = topo_tile->kind_id;
  topo_tile->execle.max_live_slots = MAX_LIVE_SLOTS;

  void * tile_mem = fd_wksp_alloc_laddr( env->mini->wksp, scratch_align(), scratch_footprint( topo_tile ), TOPO_TAG );
  FD_TEST( tile_mem );
  env->tile_mem = tile_mem;
  topo->objs[ topo_tile->tile_obj_id ].offset = fd_wksp_gaddr_fast( env->mini->wksp, tile_mem );

  fd_topo_link_t * pack_execle = fd_topob_link( topo, "pack_execle", "execle", 32UL, MAX_MICROBLOCK_SZ, 1UL );
  fd_topo_link_t * execle_poh  = fd_topob_link( topo, "execle_poh",  "execle", 32UL, MAX_MICROBLOCK_SZ, 1UL );
  fd_topo_link_t * execle_pack = fd_topob_link( topo, "execle_pack", "execle", 32UL, MAX_MICROBLOCK_SZ, 1UL );
  fd_topo_link_t * bank_bam    = fd_topob_link( topo, "bank_bam",    "execle", 32UL, sizeof(fd_bam_bundle_result_t), 1UL );
  test_topo_link_init( env, topo, pack_execle );
  test_topo_link_init( env, topo, execle_poh  );
  test_topo_link_init( env, topo, execle_pack );
  test_topo_link_init( env, topo, bank_bam    );
  fd_topob_tile_in ( topo, "execle", kind_id, "execle", "pack_execle", kind_id, FD_TOPOB_RELIABLE, FD_TOPOB_POLLED );
  fd_topob_tile_out( topo, "execle", kind_id, "execle_poh",  kind_id );
  fd_topob_tile_out( topo, "execle", kind_id, "execle_pack", kind_id );
  fd_topob_tile_out( topo, "execle", kind_id, "bank_bam",    kind_id );

  /* Share mini's accounts DB with the tile.  The tile re-joins the same
     accdb shmem (a second writer joiner) and opens it via the well-known
     FD_ACCDB_FD_RW fd, so we dup mini's backing memfd onto it. */
  FD_TEST( dup2( env->mini->accdb_fd, FD_ACCDB_FD_RW )==FD_ACCDB_FD_RW );

  fd_topo_obj_t * accdb_obj      = test_topo_obj_laddr( topo, "accdb_shmem", "execle", env->mini->accdb_shmem_mem );
  fd_topo_obj_t * progcache_obj  = test_topo_obj_laddr( topo, "progcache",  "execle", env->mini->progcache->join->shmem );
  fd_topo_obj_t * banks_obj      = test_topo_obj_laddr( topo, "banks",      "execle", env->mini->banks );
  fd_topo_obj_t * txncache_obj   = test_topo_obj_laddr( topo, "txncache",   "execle", env->mini->txncache_shmem );
  if( !sibling ) FD_TEST( fd_pod_insertf_ulong( topo->props, banks_obj->id, "banks" ) );

  void * busy_fseq_mem = fd_wksp_alloc_laddr( env->mini->wksp, fd_fseq_align(), fd_fseq_footprint(), TOPO_TAG );
  FD_TEST( fd_fseq_new( busy_fseq_mem, 0UL ) );
  fd_topo_obj_t * busy_fseq_obj = test_topo_obj_laddr( topo, "fseq", "execle", busy_fseq_mem );
  FD_TEST( fd_pod_insertf_ulong( topo->props, busy_fseq_obj->id, "execle_busy.%lu", topo_tile->kind_id ) );

  topo_tile->execle.accdb_obj_id     = accdb_obj->id;
  topo_tile->execle.progcache_obj_id = progcache_obj->id;
  topo_tile->execle.txncache_obj_id  = txncache_obj->id;

  privileged_init( topo, topo_tile );
  unprivileged_init( topo, topo_tile );

  env->execle = tile_mem;
  env->execle->pack_in_mem    = pack_execle->dcache;
  env->execle->pack_in_chunk0 = fd_dcache_compact_chunk0( pack_execle->dcache, pack_execle->dcache );
  env->execle->pack_in_wmark  = fd_dcache_compact_wmark ( pack_execle->dcache, pack_execle->dcache, pack_execle->mtu );

  env->execle->out_poh->mem    = execle_poh->dcache;
  env->execle->out_poh->chunk0 = fd_dcache_compact_chunk0( execle_poh->dcache, execle_poh->dcache );
  env->execle->out_poh->wmark  = fd_dcache_compact_wmark ( execle_poh->dcache, execle_poh->dcache, execle_poh->mtu );
  env->execle->out_poh->chunk  = env->execle->out_poh->chunk0;

  env->execle->out_pack->mem    = execle_pack->dcache;
  env->execle->out_pack->chunk0 = fd_dcache_compact_chunk0( execle_pack->dcache, execle_pack->dcache );
  env->execle->out_pack->wmark  = fd_dcache_compact_wmark ( execle_pack->dcache, execle_pack->dcache, execle_pack->mtu );
  env->execle->out_pack->chunk  = env->execle->out_pack->chunk0;
  return env;
}

static test_env_t *
test_env_create( void ) {
  return test_env_create_worker( NULL );
}

static void
test_env_destroy( test_env_t * env ) {
  ulong tag = TOPO_TAG;
  fd_wksp_tag_free( env->mini->wksp, &tag, 1UL );
}

static void
test_build_vote_txn( fd_txn_p_t * out,
                     fd_bank_t *   bank ) {
  fd_pubkey_t identity_key;
  fd_pubkey_t vote_key;
  test_mock_validator_keys( &identity_key, &vote_key );

  fd_acct_addr_t const vote_prog_id = { .b = { VOTE_PROG_ID } };
  fd_hash_t const *    recent_blockhash    = fd_blockhashes_peek_last_hash( &bank->f.block_hash_queue );
  FD_TEST( recent_blockhash );

  uchar const vote_data[] = {
    0x0e,0x00,0x00,0x00,
  };

  fd_txn_builder_t builder[1];
  FD_TEST( fd_txn_builder_new( builder, 1UL ) );
  FD_TEST( fd_txn_builder_fee_payer_set( builder, &identity_key ) );
  fd_txn_builder_blockhash_set( builder, recent_blockhash );
  FD_TEST( fd_txn_builder_instr_open( builder, &vote_prog_id, vote_data, sizeof(vote_data) ) );
  FD_TEST( fd_txn_builder_instr_account_push( builder, &vote_key,    FD_TXN_ACCT_CAT_WRITABLE ) );
  FD_TEST( fd_txn_builder_instr_account_push( builder, &identity_key, FD_TXN_ACCT_CAT_SIGNER   ) );
  fd_txn_builder_instr_close( builder );

  fd_memset( out, 0, sizeof(fd_txn_p_t) );
  FD_TEST( fd_txn_build_p( builder, out ) );
  FD_TEST( fd_txn_is_simple_vote_transaction( TXN(out), out->payload ) );
  out->pack_cu.non_execution_cus                 = 1000U;
  out->pack_cu.requested_exec_plus_acct_data_cus = 300000U;
  out->flags = FD_TXN_P_FLAGS_IS_SIMPLE_VOTE;
  fd_txn_builder_delete( builder );
}

static void
test_build_vote_authorize_txn( fd_txn_p_t * out,
                               fd_bank_t *   bank ) {
  fd_pubkey_t identity_key;
  fd_pubkey_t vote_key;
  test_mock_validator_keys( &identity_key, &vote_key );

  fd_pubkey_t new_authority = { .ul = { 0xa17a0UL } };
  fd_acct_addr_t const vote_prog_id = { .b = { VOTE_PROG_ID } };
  fd_hash_t const * recent_blockhash = fd_blockhashes_peek_last_hash( &bank->f.block_hash_queue );
  FD_TEST( recent_blockhash );

  uchar instr_data[ 40UL ];
  uint discriminant = fd_vote_instruction_enum_authorize;
  uint authorization_type = fd_vote_authorize_enum_voter;
  fd_memcpy( instr_data,       &discriminant,       sizeof(uint) );
  fd_memcpy( instr_data+4UL,   &new_authority,      sizeof(fd_pubkey_t) );
  fd_memcpy( instr_data+36UL,  &authorization_type, sizeof(uint) );

  fd_txn_builder_t builder[1];
  FD_TEST( fd_txn_builder_new( builder, 2UL ) );
  FD_TEST( fd_txn_builder_fee_payer_set( builder, &identity_key ) );
  fd_txn_builder_blockhash_set( builder, recent_blockhash );
  FD_TEST( fd_txn_builder_instr_open( builder, &vote_prog_id, instr_data, sizeof(instr_data) ) );
  FD_TEST( fd_txn_builder_instr_account_push( builder, &vote_key,        FD_TXN_ACCT_CAT_WRITABLE ) );
  FD_TEST( fd_txn_builder_instr_account_push( builder, &fd_sysvar_clock_id, 0U ) );
  FD_TEST( fd_txn_builder_instr_account_push( builder, &identity_key,    FD_TXN_ACCT_CAT_SIGNER   ) );
  fd_txn_builder_instr_close( builder );

  fd_memset( out, 0, sizeof(fd_txn_p_t) );
  FD_TEST( fd_txn_build_p( builder, out ) );
  FD_TEST( fd_txn_is_simple_vote_transaction( TXN(out), out->payload ) );
  out->pack_cu.non_execution_cus                 = 1000U;
  out->pack_cu.requested_exec_plus_acct_data_cus = 300000U;
  out->flags = FD_TXN_P_FLAGS_IS_SIMPLE_VOTE;
  fd_txn_builder_delete( builder );
}

#define TEST_CHECKED_ADD_TO_TXN_DATA( _begin, _cur_data, _to_add, _sz ) __extension__({ \
  if( FD_UNLIKELY( (*_cur_data)+(_sz)>(_begin)+FD_TXN_MTU ) ) return ULONG_MAX;         \
  fd_memcpy( *_cur_data, _to_add, (_sz) );                                              \
  *_cur_data += (_sz);                                                                  \
})

#define TEST_CHECKED_ADD_CU16_TO_TXN_DATA( _begin, _cur_data, _to_add ) __extension__({ \
  do {                                                                                  \
    uchar _buf[3];                                                                      \
    ulong _sz = (ulong)fd_cu16_enc( (ushort)(_to_add), _buf );                          \
    TEST_CHECKED_ADD_TO_TXN_DATA( _begin, _cur_data, _buf, _sz );                       \
  } while(0);                                                                           \
})

static ulong
test_txn_serialize_empty( uchar *          txn_raw_begin,
                          fd_signature_t * signature,
                          ulong            readonly_signed_cnt,
                          ulong            readonly_unsigned_cnt,
                          fd_pubkey_t *    account_keys,
                          ulong            account_key_cnt,
                          fd_hash_t const * recent_blockhash ) {
  uchar * txn_raw_cur = txn_raw_begin;

  uchar signature_cnt = 1U;
  FD_TEST( readonly_signed_cnt  <=(ulong)UCHAR_MAX );
  FD_TEST( readonly_unsigned_cnt<=(ulong)UCHAR_MAX );
  uchar header_readonly_signed_cnt   = (uchar)readonly_signed_cnt;
  uchar header_readonly_unsigned_cnt = (uchar)readonly_unsigned_cnt;

  TEST_CHECKED_ADD_TO_TXN_DATA( txn_raw_begin, &txn_raw_cur, &signature_cnt, sizeof(uchar) );
  TEST_CHECKED_ADD_TO_TXN_DATA( txn_raw_begin, &txn_raw_cur, signature, FD_TXN_SIGNATURE_SZ );

  uchar header_b0 = (uchar)0x80UL;
  TEST_CHECKED_ADD_TO_TXN_DATA( txn_raw_begin, &txn_raw_cur, &header_b0, sizeof(uchar) );
  TEST_CHECKED_ADD_TO_TXN_DATA( txn_raw_begin, &txn_raw_cur, &signature_cnt, sizeof(uchar) );
  TEST_CHECKED_ADD_TO_TXN_DATA( txn_raw_begin, &txn_raw_cur, &header_readonly_signed_cnt, sizeof(uchar) );
  TEST_CHECKED_ADD_TO_TXN_DATA( txn_raw_begin, &txn_raw_cur, &header_readonly_unsigned_cnt, sizeof(uchar) );

  TEST_CHECKED_ADD_CU16_TO_TXN_DATA( txn_raw_begin, &txn_raw_cur, account_key_cnt );
  for( ulong i=0UL; i<account_key_cnt; i++ )
    TEST_CHECKED_ADD_TO_TXN_DATA( txn_raw_begin, &txn_raw_cur, &account_keys[i], sizeof(fd_pubkey_t) );

  TEST_CHECKED_ADD_TO_TXN_DATA( txn_raw_begin, &txn_raw_cur, recent_blockhash, sizeof(fd_hash_t) );

  ushort instr_cnt = 0U;
  TEST_CHECKED_ADD_CU16_TO_TXN_DATA( txn_raw_begin, &txn_raw_cur, instr_cnt );

  ushort addr_table_cnt = 0U;
  TEST_CHECKED_ADD_CU16_TO_TXN_DATA( txn_raw_begin, &txn_raw_cur, addr_table_cnt );

  return (ulong)( txn_raw_cur - txn_raw_begin );
}

static void
test_build_empty_txn( fd_txn_p_t *    out,
                      fd_bank_t *      bank,
                      fd_pubkey_t      fee_payer,
                      fd_pubkey_t      extra_acct,
                      ulong            signature_seed,
                      int              extra_readonly ) {
  fd_signature_t signature = {0};
  signature.ul[0] = signature_seed;

  fd_pubkey_t account_keys[2] = { fee_payer, extra_acct };
  fd_memset( out, 0, sizeof(fd_txn_p_t) );
  fd_hash_t const * recent_blockhash = fd_blockhashes_peek_last_hash( &bank->f.block_hash_queue );
  FD_TEST( recent_blockhash );
  ulong sz = test_txn_serialize_empty( out->payload, &signature, 0UL, (ulong)!!extra_readonly,
                                       account_keys, 2UL, recent_blockhash );
  FD_TEST( sz!=ULONG_MAX );
  FD_TEST( fd_txn_parse( out->payload, sz, TXN( out ), NULL ) );
  out->payload_sz = (ushort)sz;
  out->pack_cu.non_execution_cus                 = 1000U;
  out->pack_cu.requested_exec_plus_acct_data_cus = 300000U;
}

static void
test_build_system_transfer_txns( fd_txn_p_t *       out,
                                 fd_bank_t *         bank,
                                 fd_pubkey_t         from,
                                 fd_pubkey_t const * to,
                                 ulong const *       lamports,
                                 ulong               transfer_cnt ) {
  fd_hash_t const * recent_blockhash = fd_blockhashes_peek_last_hash( &bank->f.block_hash_queue );
  FD_TEST( recent_blockhash );

  fd_txn_builder_t builder[1];
  FD_TEST( fd_txn_builder_new( builder, transfer_cnt+1UL ) );
  FD_TEST( fd_txn_builder_fee_payer_set( builder, &from ) );
  fd_txn_builder_blockhash_set( builder, recent_blockhash );
  for( ulong i=0UL; i<transfer_cnt; i++ ) {
    fd_system_program_instruction_t instr = { .discriminant = FD_SYSTEM_PROGRAM_INSTR_TRANSFER,
                                              .inner.transfer = lamports[ i ] };
    uchar instr_data[ 16 ];
    ulong instr_data_sz = 0UL;
    FD_TEST( !fd_system_program_instruction_encode( &instr, instr_data, sizeof(instr_data), &instr_data_sz ) );
    FD_TEST( fd_txn_builder_instr_open( builder, &fd_solana_system_program_id, instr_data, instr_data_sz ) );
    FD_TEST( fd_txn_builder_instr_account_push( builder, &from,  FD_TXN_ACCT_CAT_WRITABLE | FD_TXN_ACCT_CAT_SIGNER ) );
    FD_TEST( fd_txn_builder_instr_account_push( builder, &to[i], FD_TXN_ACCT_CAT_WRITABLE ) );
    fd_txn_builder_instr_close( builder );
  }

  fd_memset( out, 0, sizeof(fd_txn_p_t) );
  FD_TEST( fd_txn_build_p( builder, out ) );
  out->pack_cu.non_execution_cus                 = 1000U;
  out->pack_cu.requested_exec_plus_acct_data_cus = 300000U;
  fd_txn_builder_delete( builder );
}

static void
test_build_system_transfer_txn( fd_txn_p_t * out,
                                fd_bank_t *   bank,
                                fd_pubkey_t   from,
                                fd_pubkey_t   to,
                                ulong         lamports ) {
  test_build_system_transfer_txns( out, bank, from, &to, &lamports, 1UL );
}

static void
test_build_missing_program_txn( fd_txn_p_t * out,
                                fd_bank_t *   bank,
                                fd_pubkey_t   fee_payer,
                                fd_pubkey_t   missing_program ) {
  fd_hash_t const * recent_blockhash = fd_blockhashes_peek_last_hash( &bank->f.block_hash_queue );
  FD_TEST( recent_blockhash );

  fd_txn_builder_t builder[1];
  FD_TEST( fd_txn_builder_new( builder, 5UL ) );
  FD_TEST( fd_txn_builder_fee_payer_set( builder, &fee_payer ) );
  fd_txn_builder_blockhash_set( builder, recent_blockhash );
  FD_TEST( fd_txn_builder_instr_open( builder, &missing_program, NULL, 0UL ) );
  fd_txn_builder_instr_close( builder );

  fd_memset( out, 0, sizeof(fd_txn_p_t) );
  FD_TEST( fd_txn_build_p( builder, out ) );
  out->pack_cu.non_execution_cus                 = 1000U;
  out->pack_cu.requested_exec_plus_acct_data_cus = 300000U;
  fd_txn_builder_delete( builder );
}

FD_FN_UNUSED static void
test_durable_nonce_from_blockhash( fd_hash_t *       out,
                                   fd_hash_t const * blockhash ) {
  uchar buf[ 13UL + sizeof(fd_hash_t) ];
  fd_memcpy( buf,      "DURABLE_NONCE", 13UL );
  fd_memcpy( buf+13UL, blockhash,       sizeof(fd_hash_t) );
  fd_sha256_hash( buf, sizeof(buf), out );
}

FD_FN_UNUSED static void
test_build_durable_nonce_transfer_txn( fd_txn_p_t *    out,
                                       fd_pubkey_t      fee_payer,
                                       fd_pubkey_t      nonce_key,
                                       fd_pubkey_t      recipient,
                                       fd_hash_t const * durable_nonce,
                                       ulong            lamports,
                                       ulong            seed ) {
  fd_system_program_instruction_t instr = {
    .discriminant   = FD_SYSTEM_PROGRAM_INSTR_TRANSFER,
    .inner.transfer = lamports
  };
  uchar instr_data[ 16 ];
  ulong instr_data_sz = 0UL;
  FD_TEST( !fd_system_program_instruction_encode( &instr, instr_data, sizeof(instr_data), &instr_data_sz ) );

  fd_txn_builder_t builder[1];
  FD_TEST( fd_txn_builder_new( builder, seed ) );
  FD_TEST( fd_txn_builder_fee_payer_set( builder, &fee_payer ) );
  fd_txn_builder_blockhash_set( builder, durable_nonce );
  FD_TEST( fd_txn_builder_nonce_set( builder, &nonce_key, &fee_payer ) );
  FD_TEST( fd_txn_builder_instr_open( builder, &fd_solana_system_program_id, instr_data, instr_data_sz ) );
  FD_TEST( fd_txn_builder_instr_account_push( builder, &fee_payer, FD_TXN_ACCT_CAT_WRITABLE | FD_TXN_ACCT_CAT_SIGNER ) );
  FD_TEST( fd_txn_builder_instr_account_push( builder, &recipient, FD_TXN_ACCT_CAT_WRITABLE ) );
  fd_txn_builder_instr_close( builder );

  fd_memset( out, 0, sizeof(fd_txn_p_t) );
  FD_TEST( fd_txn_build_p( builder, out ) );
  out->pack_cu.non_execution_cus                 = 1000U;
  out->pack_cu.requested_exec_plus_acct_data_cus = 300000U;
  out->flags = FD_TXN_P_FLAGS_DURABLE_NONCE;
  fd_txn_builder_delete( builder );
}

FD_FN_UNUSED static void
test_build_bpf_close_txn( fd_txn_p_t * out,
                          fd_bank_t *   bank,
                          fd_pubkey_t   authority,
                          fd_pubkey_t   recipient,
                          fd_pubkey_t   program,
                          fd_pubkey_t   programdata ) {
  fd_bpf_instruction_t instr = { .discriminant = FD_BPF_INSTR_CLOSE };
  uchar instr_data[ 4 ];
  ulong instr_data_sz = 0UL;
  FD_TEST( !fd_bpf_instruction_encode( &instr, instr_data, sizeof(instr_data), &instr_data_sz ) );

  fd_hash_t const * recent_blockhash = fd_blockhashes_peek_last_hash( &bank->f.block_hash_queue );
  FD_TEST( recent_blockhash );

  fd_txn_builder_t builder[1];
  FD_TEST( fd_txn_builder_new( builder, 3UL ) );
  FD_TEST( fd_txn_builder_fee_payer_set( builder, &authority ) );
  fd_txn_builder_blockhash_set( builder, recent_blockhash );
  FD_TEST( fd_txn_builder_instr_open( builder, &fd_solana_bpf_loader_upgradeable_program_id, instr_data, instr_data_sz ) );
  FD_TEST( fd_txn_builder_instr_account_push( builder, &programdata, FD_TXN_ACCT_CAT_WRITABLE ) );
  FD_TEST( fd_txn_builder_instr_account_push( builder, &recipient,   FD_TXN_ACCT_CAT_WRITABLE ) );
  FD_TEST( fd_txn_builder_instr_account_push( builder, &authority,   FD_TXN_ACCT_CAT_SIGNER   ) );
  FD_TEST( fd_txn_builder_instr_account_push( builder, &program,     FD_TXN_ACCT_CAT_WRITABLE ) );
  fd_txn_builder_instr_close( builder );

  fd_memset( out, 0, sizeof(fd_txn_p_t) );
  FD_TEST( fd_txn_build_p( builder, out ) );
  out->pack_cu.non_execution_cus                 = 1000U;
  out->pack_cu.requested_exec_plus_acct_data_cus = 300000U;
  fd_txn_builder_delete( builder );
}

FD_FN_UNUSED static void
test_build_program_invoke_txn( fd_txn_p_t * out,
                               fd_bank_t *   bank,
                               fd_pubkey_t   fee_payer,
                               fd_pubkey_t   program ) {
  fd_hash_t const * recent_blockhash = fd_blockhashes_peek_last_hash( &bank->f.block_hash_queue );
  FD_TEST( recent_blockhash );

  fd_txn_builder_t builder[1];
  FD_TEST( fd_txn_builder_new( builder, 4UL ) );
  FD_TEST( fd_txn_builder_fee_payer_set( builder, &fee_payer ) );
  fd_txn_builder_blockhash_set( builder, recent_blockhash );
  FD_TEST( fd_txn_builder_instr_open( builder, &program, NULL, 0UL ) );
  fd_txn_builder_instr_close( builder );

  fd_memset( out, 0, sizeof(fd_txn_p_t) );
  FD_TEST( fd_txn_build_p( builder, out ) );
  out->pack_cu.non_execution_cus                 = 1000U;
  out->pack_cu.requested_exec_plus_acct_data_cus = 300000U;
  fd_txn_builder_delete( builder );
}

static void
test_fund_account( test_env_t *          env,
                   fd_pubkey_t const *   pubkey,
                   ulong                 lamports ) {
  fd_accdb_fork_id_t fork_id = fd_svm_mini_fork_id( env->mini, env->bank_idx );
  fd_svm_mini_add_lamports( env->mini, fork_id, pubkey, lamports );
}

static void
test_put_account_rooted( test_env_t *        env,
                         fd_pubkey_t const * pubkey,
                         fd_pubkey_t const * owner,
                         ulong               lamports,
                         ulong               slot,
                         int                 executable,
                         uchar const *       data,
                         ulong               data_sz ) {
  (void)slot;
  fd_acc_t acc = {0};
  fd_memcpy( acc.pubkey, pubkey, sizeof(fd_pubkey_t) );
  fd_memcpy( acc.owner,  owner,  sizeof(fd_pubkey_t) );
  acc.lamports   = lamports;
  acc.executable = !!executable;
  acc.data_len   = data_sz;
  acc.data       = (uchar *)data;
  fd_svm_mini_put_account_rooted( env->mini, &acc );
}

FD_FN_UNUSED static void
test_put_nonce_account_rooted( test_env_t *        env,
                               fd_pubkey_t const * nonce_key,
                               fd_pubkey_t const * authority,
                               fd_hash_t const *   durable_nonce,
                               ulong               lamports ) {
  fd_nonce_state_versions_t state = {
    .version       = FD_NONCE_VERSION_CURRENT,
    .kind          = FD_NONCE_STATE_INITIALIZED,
    .authority     = *authority,
    .durable_nonce = *durable_nonce,
  };
  uchar data[ FD_SYSTEM_PROGRAM_NONCE_DLEN ] = {0};
  ulong written = 0UL;
  FD_TEST( !fd_nonce_state_versions_encode( &state, data, FD_SYSTEM_PROGRAM_NONCE_DLEN, &written ) );
  test_put_account_rooted( env, nonce_key, &fd_solana_system_program_id, lamports, 0UL, 0,
                           data, FD_SYSTEM_PROGRAM_NONCE_DLEN );
}

static ulong
test_read_lamports( test_env_t *        env,
                    fd_pubkey_t const * pubkey ) {
  fd_accdb_fork_id_t fork_id = fd_svm_mini_fork_id( env->mini, env->bank_idx );
  return fd_accdb_lamports( env->mini->runtime->accdb, fork_id, pubkey->uc );
}

static fd_stem_context_t *
test_stem( fd_execle_tile_t * ctx,
           fd_stem_context_t * stem ) {
  static fd_frag_meta_t * mcaches[3];
  static ulong            seqs[3];
  static ulong            depths[3];
  static ulong            cr_avail[3];
  static ulong            min_cr_avail;
  static int              out_reliable[3];

  fd_topo_link_t const * execle_poh  = test_topo_link( "execle_poh"  );
  fd_topo_link_t const * execle_pack = test_topo_link( "execle_pack" );
  fd_topo_link_t const * bank_bam    = test_topo_link( "bank_bam"    );

  mcaches[ ctx->out_poh->idx  ] = execle_poh->mcache;
  mcaches[ ctx->out_pack->idx ] = execle_pack->mcache;
  mcaches[ ctx->out_bam->idx  ] = bank_bam->mcache;
  depths [ ctx->out_poh->idx  ] = execle_poh->depth;
  depths [ ctx->out_pack->idx ] = execle_pack->depth;
  depths [ ctx->out_bam->idx  ] = bank_bam->depth;
  seqs   [ ctx->out_poh->idx  ] = fd_mcache_seq_query( fd_mcache_seq_laddr_const( mcaches[ ctx->out_poh->idx  ] ) );
  seqs   [ ctx->out_pack->idx ] = fd_mcache_seq_query( fd_mcache_seq_laddr_const( mcaches[ ctx->out_pack->idx ] ) );
  seqs   [ ctx->out_bam->idx  ] = fd_mcache_seq_query( fd_mcache_seq_laddr_const( mcaches[ ctx->out_bam->idx  ] ) );
  cr_avail[0] = cr_avail[1] = cr_avail[2] = ULONG_MAX;
  min_cr_avail = ULONG_MAX;
  out_reliable[0] = out_reliable[1] = out_reliable[2] = 0;

  *stem = (fd_stem_context_t) {
    .mcaches             = mcaches,
    .seqs                = seqs,
    .depths              = depths,
    .cr_avail            = cr_avail,
    .min_cr_avail        = &min_cr_avail,
    .cr_decrement_amount = 1UL,
    .out_reliable        = out_reliable,
  };
  return stem;
}

static void
test_execle_flush_rebate( test_env_t * env ) {
  fd_stem_context_t stem[1];
  int opt_poll_in = 1;
  int charge_busy = 0;
  env->execle->rebate_idle_loop_cnt = REBATE_BATCH_IDLE_LOOPS;
  after_credit( env->execle, test_stem( env->execle, stem ), &opt_poll_in, &charge_busy );
  FD_TEST( !opt_poll_in );
  FD_TEST( charge_busy );
}

static void
test_execle_run( test_env_t *     env,
                 fd_txn_p_t *     txns,
                 ulong            txn_cnt,
                 uint             pack_idx,
                 ulong            pack_txn_idx,
                 int              is_bundle ) {
  FD_TEST( txn_cnt<=MAX_TXN_PER_SLOT );

  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
  FD_TEST( bank );

  ulong in_chunk = env->execle->pack_in_chunk0;
  fd_txn_e_t * in_txn = fd_chunk_to_laddr( env->execle->pack_in_mem, in_chunk );
  for( ulong i=0UL; i<txn_cnt; i++ ) {
    fd_memset( &in_txn[i], 0, sizeof(fd_txn_e_t) );
    fd_memcpy( in_txn[i].txnp, &txns[i], sizeof(fd_txn_p_t) );
    in_txn[i].first_seen_nanos = 1000L+(long)pack_txn_idx+(long)i;
    if( is_bundle ) in_txn[i].txnp->flags |= FD_TXN_P_FLAGS_BUNDLE;
  }

  fd_microblock_execle_trailer_t * in_trailer = (fd_microblock_execle_trailer_t *)( in_txn+txn_cnt );
  *in_trailer = (fd_microblock_execle_trailer_t) {
    .bank_idx       = env->bank_idx,
    .microblock_idx = 0UL,
    .pack_idx       = pack_idx,
    .pack_txn_idx   = pack_txn_idx,
    .is_bundle      = is_bundle,
  };

  ulong sig = fd_disco_poh_sig( bank->f.slot, POH_PKT_TYPE_MICROBLOCK, env->execle->kind_id );
  ulong sz  = txn_cnt*sizeof(fd_txn_e_t) + sizeof(fd_microblock_execle_trailer_t);
  FD_TEST( !before_frag( env->execle, 0UL, 0UL, sig ) );
  during_frag( env->execle, 0UL, 0UL, sig, in_chunk, sz, 0UL );

  fd_stem_context_t stem[1];
  after_frag( env->execle, 0UL, 0UL, sig, sz, 0UL, fd_frag_meta_ts_comp( fd_tickcount() ), test_stem( env->execle, stem ) );
  FD_TEST( fd_fseq_query( env->execle->busy_fseq )==0UL );

  /* Ordinary microblocks retain every member's ingress time; atomic
     bundles split into one output per member without losing its time. */
  fd_topo_link_t const * out = test_topo_link( "execle_poh" );
  for( ulong i=0UL; i<(is_bundle ? txn_cnt : 1UL); i++ ) {
    fd_frag_meta_t const * meta = out->mcache + fd_mcache_line_idx( i, out->depth );
    uchar const * data = fd_chunk_to_laddr_const( env->execle->out_poh->mem, meta->chunk );
    fd_microblock_trailer_t const * trailer = (fd_microblock_trailer_t const *)(data+meta->sz-sizeof(fd_microblock_trailer_t));
    for( ulong j=0UL; j<(is_bundle ? 1UL : txn_cnt); j++ )
      FD_TEST( trailer->first_seen_nanos[j]==1000L+(long)pack_txn_idx+(long)i+(long)j );
  }
}

static void
test_mark_bam_batch( fd_txn_p_t * txns,
                     ulong        txn_cnt,
                     uint         seq_id,
                     int          revert_on_error ) {
  for( ulong i=0UL; i<txn_cnt; i++ ) {
    txns[ i ].source_tpu          = FD_TXN_M_TPU_SOURCE_BAM;
    txns[ i ].bam.seq_id          = seq_id;
    txns[ i ].bam.scheduler_gen   = 0U;
    txns[ i ].bam.batch_idx       = (uchar)i;
    txns[ i ].bam.revert_on_error = !!revert_on_error;
  }
}

static fd_frag_meta_t const *
test_out_poh_meta( ulong seq ) {
  fd_topo_link_t const * execle_poh = test_topo_link( "execle_poh" );
  return execle_poh->mcache + fd_mcache_line_idx( seq, execle_poh->depth );
}

FD_FN_UNUSED static fd_frag_meta_t const *
test_out_pack_meta( ulong seq ) {
  fd_topo_link_t const * execle_pack = test_topo_link( "execle_pack" );
  return execle_pack->mcache + fd_mcache_line_idx( seq, execle_pack->depth );
}

static void
test_assert_nonbundle_out( test_env_t * env,
                           ulong        txn_cnt,
                           uint         pack_idx ) {
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
  fd_frag_meta_t const * meta = test_out_poh_meta( 0UL );
  FD_TEST( fd_frag_meta_seq_query( meta )==0UL );
  FD_TEST( meta->sig==fd_disco_execle_sig( bank->f.slot, pack_idx ) );
  FD_TEST( meta->sz==txn_cnt*sizeof(fd_txn_p_t)+sizeof(fd_microblock_trailer_t) );
}

static void
test_assert_bundle_out( test_env_t * env,
                        ulong        txn_cnt,
                        uint         pack_idx ) {
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
  for( ulong i=0UL; i<txn_cnt; i++ ) {
    fd_frag_meta_t const * meta = test_out_poh_meta( i );
    FD_TEST( fd_frag_meta_seq_query( meta )==i );
    FD_TEST( meta->sig==fd_disco_execle_sig( bank->f.slot, pack_idx+(uint)i ) );
    FD_TEST( meta->sz==sizeof(fd_txn_p_t)+sizeof(fd_microblock_trailer_t) );
  }
}

static fd_microblock_trailer_t const *
test_out_poh_trailer_nonbundle( test_env_t * env,
                                ulong        txn_cnt ) {
  fd_frag_meta_t const * meta = test_out_poh_meta( 0UL );
  fd_txn_p_t const * txns = fd_chunk_to_laddr( env->execle->out_poh->mem, meta->chunk );
  return (fd_microblock_trailer_t const *)( txns + txn_cnt );
}

static fd_microblock_trailer_t const *
test_out_poh_trailer_bundle( test_env_t * env,
                             ulong        seq ) {
  fd_frag_meta_t const * meta = test_out_poh_meta( seq );
  uchar const * data = fd_chunk_to_laddr( env->execle->out_poh->mem, meta->chunk );
  return (fd_microblock_trailer_t const *)(data+meta->sz-sizeof(fd_microblock_trailer_t));
}

static void
test_compute_expected_hash( fd_txn_p_t * txns,
                            ulong        txn_cnt,
                            uchar        expected_hash[32] ) {
  uchar bmtree_mem[ FD_BMTREE_COMMIT_FOOTPRINT(0) ] __attribute__((aligned(FD_BMTREE_COMMIT_ALIGN)));
  hash_transactions( bmtree_mem, txns, txn_cnt, expected_hash );
}

static void
test_assert_txn_ns_dt_ordered( fd_txn_ns_dt_t const * dt ) {
  FD_TEST( dt->load_start   >= 0.f );
  FD_TEST( dt->check_start  >= dt->load_start   );
  FD_TEST( dt->exec_start   >= dt->check_start  );
  FD_TEST( dt->commit_start >= dt->exec_start   );
  FD_TEST( dt->commit_end   >= dt->commit_start );
}

FD_UNIT_TEST( execle_seccomp ) {
  int   out_fds[3];
  ulong nfds = populate_allowed_fds( NULL, NULL, 3UL, out_fds );
  FD_TEST( nfds>=2 && nfds<=3 );
  FD_TEST( out_fds[0]==STDERR_FILENO );
  /* logfile fd is optional; the accounts db fd is always last */
  FD_TEST( out_fds[ nfds-1UL ]==FD_ACCDB_FD_RW );
  if( nfds==3 ) FD_TEST( out_fds[1]==fd_log_private_logfile_fd() );

  struct sock_filter filter[ sock_filter_policy_fd_execle_tile_instr_cnt ];
  populate_allowed_seccomp( NULL, NULL, sock_filter_policy_fd_execle_tile_instr_cnt, filter );
}

FD_UNIT_TEST( execle_rebate_deferred ) {
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );

  fd_pubkey_t missing_fee_payer = { .ul = { 0x1234UL } };
  fd_pubkey_t writable_acct     = { .ul = { 0x5678UL } };
  fd_txn_p_t txn[1];
  test_build_empty_txn( txn, bank, missing_fee_payer, writable_acct, 1UL, 0 );
  test_execle_run( env, txn, 1UL, 0U, 0UL, 0 );

  FD_TEST( fd_frag_meta_seq_query( test_out_pack_meta( 0UL ) )==ULONG_MAX );

  fd_stem_context_t stem[1];
  int opt_poll_in = 1;
  int charge_busy = 0;
  after_credit( env->execle, test_stem( env->execle, stem ), &opt_poll_in, &charge_busy );
  FD_TEST( fd_frag_meta_seq_query( test_out_pack_meta( 0UL ) )==ULONG_MAX );
  FD_TEST( opt_poll_in );
  FD_TEST( !charge_busy );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_rebate_flushes_after_idle_loops ) {
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );

  fd_pubkey_t missing_fee_payer = { .ul = { 0x1234UL } };
  fd_pubkey_t writable_acct     = { .ul = { 0x5678UL } };
  fd_txn_p_t txn[1];
  test_build_empty_txn( txn, bank, missing_fee_payer, writable_acct, 1UL, 0 );
  test_execle_run( env, txn, 1UL, 0U, 0UL, 0 );

  for( ulong i=0UL; i<REBATE_BATCH_IDLE_LOOPS; i++ ) {
    fd_stem_context_t stem[1];
    int opt_poll_in = 1;
    int charge_busy = 0;
    after_credit( env->execle, test_stem( env->execle, stem ), &opt_poll_in, &charge_busy );
    FD_TEST( fd_frag_meta_seq_query( test_out_pack_meta( 0UL ) )==ULONG_MAX );
    FD_TEST( opt_poll_in );
    FD_TEST( !charge_busy );
  }

  fd_stem_context_t stem[1];
  int opt_poll_in = 1;
  int charge_busy = 0;
  after_credit( env->execle, test_stem( env->execle, stem ), &opt_poll_in, &charge_busy );
  FD_TEST( fd_frag_meta_seq_query( test_out_pack_meta( 0UL ) )==0UL );
  FD_TEST( !opt_poll_in );
  FD_TEST( charge_busy );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_rebate_flushes_full_batch ) {
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
  fd_pubkey_t writable_acct = { .ul = { 0x9876UL } };

  for( ulong i=0UL; i<REBATE_BATCH_MAX_MICROBLOCKS; i++ ) {
    fd_pubkey_t missing_fee_payer = { .ul = { 0x1000UL+i } };
    fd_txn_p_t txn[1];
    test_build_empty_txn( txn, bank, missing_fee_payer, writable_acct, 10UL+i, 0 );
    test_execle_run( env, txn, 1UL, (uint)i, i, 0 );

    fd_stem_context_t stem[1];
    int opt_poll_in = 1;
    int charge_busy = 0;
    after_credit( env->execle, test_stem( env->execle, stem ), &opt_poll_in, &charge_busy );

    if( i+1UL<REBATE_BATCH_MAX_MICROBLOCKS ) {
      FD_TEST( fd_frag_meta_seq_query( test_out_pack_meta( 0UL ) )==ULONG_MAX );
      FD_TEST( opt_poll_in );
      FD_TEST( !charge_busy );
    } else {
      FD_TEST( fd_frag_meta_seq_query( test_out_pack_meta( 0UL ) )==0UL );
      FD_TEST( !opt_poll_in );
      FD_TEST( charge_busy );
    }
  }

  fd_pack_rebate_t const * rebate = fd_chunk_to_laddr( env->execle->out_pack->mem, test_out_pack_meta( 0UL )->chunk );
  FD_TEST( rebate->total_cost_rebate==REBATE_BATCH_MAX_MICROBLOCKS*301000UL );
  FD_TEST( rebate->writer_cnt==(uint)(REBATE_BATCH_MAX_MICROBLOCKS+1UL) );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_rebate_batch_counts_bundle_microblocks ) {
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
  fd_pubkey_t writable_acct = { .ul = { 0xaaaaUL } };

  fd_txn_p_t bundle[5];
  for( ulong i=0UL; i<5UL; i++ ) {
    fd_pubkey_t missing_fee_payer = { .ul = { 0x2000UL+i } };
    test_build_empty_txn( bundle+i, bank, missing_fee_payer, writable_acct, 30UL+i, 0 );
  }
  test_execle_run( env, bundle, 5UL, 0U, 0UL, 1 );

  fd_stem_context_t stem[1];
  int opt_poll_in = 1;
  int charge_busy = 0;
  after_credit( env->execle, test_stem( env->execle, stem ), &opt_poll_in, &charge_busy );
  FD_TEST( fd_frag_meta_seq_query( test_out_pack_meta( 0UL ) )==0UL );
  FD_TEST( !opt_poll_in );
  FD_TEST( charge_busy );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_rebate_flushes_initializer_bundle ) {
  test_env_t * env = test_env_create();

  fd_txn_p_t txn[1] = {0};
  txn->flags = FD_TXN_P_FLAGS_BUNDLE | FD_TXN_P_FLAGS_INITIALIZER_BUNDLE | FD_TXN_P_FLAGS_EXECUTE_SUCCESS;
  fd_acct_addr_t const * alt_ptr[1] = { NULL };
  FD_TEST( !fd_pack_rebate_sum_add_txn( env->execle->rebater, txn, alt_ptr, 1UL ) );

  env->execle->rebate_microblock_cnt = 1UL;

  fd_stem_context_t stem[1];
  int opt_poll_in = 1;
  int charge_busy = 0;
  after_credit( env->execle, test_stem( env->execle, stem ), &opt_poll_in, &charge_busy );

  FD_TEST( fd_frag_meta_seq_query( test_out_pack_meta( 0UL ) )==0UL );
  FD_TEST( !opt_poll_in );
  FD_TEST( charge_busy );

  fd_pack_rebate_t const * rebate = fd_chunk_to_laddr( env->execle->out_pack->mem, test_out_pack_meta( 0UL )->chunk );
  FD_TEST( rebate->ib_result==1 );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_rebate_slot_change_resets_batch ) {
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
  fd_pubkey_t writable_acct = { .ul = { 0xabcdUL } };

  fd_pubkey_t first_fee_payer = { .ul = { 0x1111UL } };
  fd_txn_p_t first_txn[1];
  test_build_empty_txn( first_txn, bank, first_fee_payer, writable_acct, 20UL, 0 );
  test_execle_run( env, first_txn, 1UL, 0U, 0UL, 0 );

  env->execle->rebate_microblock_cnt = REBATE_BATCH_MAX_MICROBLOCKS-1UL;

  bank->f.slot++;
  fd_pubkey_t second_fee_payer = { .ul = { 0x2222UL } };
  fd_txn_p_t second_txn[1];
  test_build_empty_txn( second_txn, bank, second_fee_payer, writable_acct, 21UL, 0 );
  test_execle_run( env, second_txn, 1UL, 1U, 1UL, 0 );

  fd_stem_context_t stem[1];
  int opt_poll_in = 1;
  int charge_busy = 0;
  after_credit( env->execle, test_stem( env->execle, stem ), &opt_poll_in, &charge_busy );

  FD_TEST( fd_frag_meta_seq_query( test_out_pack_meta( 0UL ) )==ULONG_MAX );
  FD_TEST( opt_poll_in );
  FD_TEST( !charge_busy );

  test_env_destroy( env );
}

/* Folding transaction or loaded-data costs into consumed_cus overcharges work
   that did not execute.  Keep 150 execution CUs and 206 loaded bytes separate. */
FD_UNIT_TEST( execle_bam_result_cus ) {
  fd_bam_bundle_result_t res[1];
  fd_txn_out_t           txn_out[1];
  fd_memset( res,     0, sizeof(res)     );
  fd_memset( txn_out, 0, sizeof(txn_out) );

  txn_out->details.compute_budget.compute_unit_limit = 200UL;
  txn_out->details.compute_budget.compute_meter      = 50UL;
  txn_out->details.txn_cost.transaction.loaded_accounts_data_size_cost = 8U;
  txn_out->details.loaded_accounts_data_size = 206UL;

  bam_fill_txn_result( res, 0UL, txn_out );

  FD_TEST( res->consumed_cus[ 0 ]==150U );
  FD_TEST( res->loaded_accounts_data_size[ 0 ]==206U );
}

/* Finalizing a transaction hash when every member failed invokes the bmtree
   finalizer with zero leaves.  Skip finalization and return the zero hash. */
FD_UNIT_TEST( execle_empty_transaction_hash ) {
  fd_txn_p_t txns[ 2 ];
  uchar      hash[ 32 ];
  fd_memset( txns, 0, sizeof(txns) );
  fd_memset( hash,  0xff, sizeof(hash) );

  test_compute_expected_hash( txns, 2UL, hash );
  FD_TEST( fd_memeq( hash, (uchar[32]){0}, sizeof(hash) ) );
}

FD_UNIT_TEST( execle_vote ) {
  /* Simple vote transaction */
  test_env_t * env = test_env_create();

  FD_TEST( env->execle->banks==env->mini->banks );
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
  FD_TEST( bank );

  ulong in_chunk = env->execle->pack_in_chunk0;
  fd_txn_e_t * in_txn = fd_chunk_to_laddr( env->execle->pack_in_mem, in_chunk );
  test_build_vote_txn( in_txn->txnp, bank );
  fd_memset( in_txn->alt_accts, 0, sizeof(in_txn->alt_accts) );

  fd_microblock_execle_trailer_t * in_trailer = (fd_microblock_execle_trailer_t *)( in_txn+1UL );
  *in_trailer = (fd_microblock_execle_trailer_t) {
    .bank_idx       = env->bank_idx,
    .microblock_idx = 0UL,
    .pack_idx       = 0U,
    .pack_txn_idx   = 0UL,
    .is_bundle      = 0,
  };

  ulong sig = fd_disco_poh_sig( bank->f.slot, POH_PKT_TYPE_MICROBLOCK, env->execle->kind_id );
  ulong sz  = sizeof(fd_txn_e_t) + sizeof(fd_microblock_execle_trailer_t);
  FD_TEST( !before_frag( env->execle, 0UL, 0UL, sig ) );
  during_frag( env->execle, 0UL, 0UL, sig, in_chunk, sz, 0UL );

  fd_stem_context_t stem[1];
  after_frag( env->execle, 0UL, 0UL, sig, sz, 0UL, fd_frag_meta_ts_comp( fd_tickcount() ), test_stem( env->execle, stem ) );

  FD_TEST( fd_fseq_query( env->execle->busy_fseq )==0UL );
  fd_topo_link_t const * execle_poh = test_topo_link( "execle_poh" );
  fd_frag_meta_t const * out_poh_mcache = execle_poh->mcache;
  fd_frag_meta_t const * out_poh_meta = out_poh_mcache + fd_mcache_line_idx( 0UL, execle_poh->depth );
  FD_TEST( fd_frag_meta_seq_query( out_poh_meta )==0UL );
  FD_TEST( out_poh_meta->sig==fd_disco_execle_sig( bank->f.slot, 0U ) );
  FD_TEST( out_poh_meta->sz==sizeof(fd_txn_p_t)+sizeof(fd_microblock_trailer_t) );

  fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, out_poh_meta->chunk );
  FD_TEST( out_txn->payload_sz==in_txn->txnp->payload_sz );
  FD_TEST( fd_txn_is_simple_vote_transaction( TXN(out_txn), out_txn->payload ) );
  FD_TEST( env->execle->txn_out[0].err.is_committable );
  FD_TEST( !env->execle->txn_out[0].err.is_fees_only );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR );
  FD_TEST( (out_txn->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR)<<24) );
  FD_TEST( out_txn->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS );
  FD_TEST( out_txn->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS );
  FD_TEST( out_txn->execle_cu.actual_consumed_cus + out_txn->execle_cu.rebated_cus ==
           in_txn->txnp->pack_cu.non_execution_cus + in_txn->txnp->pack_cu.requested_exec_plus_acct_data_cus );
  FD_TEST( out_txn->execle_cu.actual_consumed_cus==
           in_txn->txnp->pack_cu.non_execution_cus +
           env->execle->txn_out[0].details.compute_budget.compute_unit_limit -
           env->execle->txn_out[0].details.compute_budget.compute_meter +
           env->execle->txn_out[0].details.txn_cost.transaction.loaded_accounts_data_size_cost );

  fd_microblock_trailer_t const * trailer = test_out_poh_trailer_nonbundle( env, 1UL );
  FD_TEST( trailer->pack_txn_idx==0UL );
  FD_TEST( trailer->tips==0UL );
  fd_txn_p_t txn_copy = *out_txn;
  uchar expected_hash[32];
  test_compute_expected_hash( &txn_copy, 1UL, expected_hash );
  FD_TEST( !memcmp( trailer->hash, expected_hash, 32UL ) );
  test_assert_txn_ns_dt_ordered( &trailer->txn_ns_dt );

  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_LANDED_FAILED_IDX ]==1UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_INSTRUCTION_ERROR_IDX ]==1UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_bundle_vote_authorize ) {
  /* Simple vote transaction in a bundle */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
  FD_TEST( bank );

  fd_txn_p_t txn[1];
  test_build_vote_authorize_txn( txn, bank );
  test_execle_run( env, txn, 1UL, 2U, 16UL, 1 );

  test_assert_bundle_out( env, 1UL, 2U );
  fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 0UL )->chunk );
  FD_TEST( fd_txn_is_simple_vote_transaction( TXN(out_txn), out_txn->payload ) );
  FD_TEST( env->execle->txn_out[0].err.is_committable );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_EXECUTE_SUCCESS );
  FD_TEST( out_txn->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS );
  FD_TEST( out_txn->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS );
  ulong actual_consumed_cus =
      txn->pack_cu.non_execution_cus +
      env->execle->txn_out[0].details.compute_budget.compute_unit_limit -
      env->execle->txn_out[0].details.compute_budget.compute_meter +
      env->execle->txn_out[0].details.txn_cost.transaction.loaded_accounts_data_size_cost;
  FD_TEST( out_txn->execle_cu.actual_consumed_cus==actual_consumed_cus );
  FD_TEST( out_txn->execle_cu.rebated_cus==
           txn->pack_cu.non_execution_cus + txn->pack_cu.requested_exec_plus_acct_data_cus - actual_consumed_cus );
  FD_TEST( !env->execle->txn_out[0].err.is_fees_only );

  fd_microblock_trailer_t const * trailer = test_out_poh_trailer_bundle( env, 0UL );
  FD_TEST( trailer->pack_txn_idx==16UL );
  FD_TEST( trailer->tips==0UL );
  fd_txn_p_t txn_copy = *out_txn;
  uchar expected_hash[32];
  test_compute_expected_hash( &txn_copy, 1UL, expected_hash );
  FD_TEST( !memcmp( trailer->hash, expected_hash, 32UL ) );
  test_assert_txn_ns_dt_ordered( &trailer->txn_ns_dt );

  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_LANDED_SUCCESS_IDX ]==1UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_SUCCESS_IDX ]==1UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_simple_ok ) {
  /* Simple system program transfer */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );

  fd_pubkey_t fee_payer = { .ul = { 0x1111UL } };
  fd_pubkey_t recipient = { .ul = { 0x2222UL } };
  ulong const payer_start     = 1000000000UL;
  ulong const recipient_start = 1UL;
  ulong const transfer        = 1234567UL;
  ulong const fee             = 5000UL;

  fd_blockhash_info_t * blockhash_info = (fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue );
  FD_TEST( blockhash_info );
  blockhash_info->lamports_per_signature = fee;

  test_fund_account( env, &fee_payer, payer_start );
  test_fund_account( env, &recipient, recipient_start );

  fd_txn_p_t txn[1];
  test_build_system_transfer_txn( txn, bank, fee_payer, recipient, transfer );
  test_mark_bam_batch( txn, 1UL, 101U, 0 );
  test_execle_run( env, txn, 1UL, 3U, 17UL, 0 );

  fd_frag_meta_t const * poh_meta = test_out_poh_meta( 0UL );
  FD_TEST( fd_frag_meta_seq_query( poh_meta )==0UL );
  FD_TEST( poh_meta->sig==fd_disco_execle_sig( bank->f.slot, 3U ) );
  fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, poh_meta->chunk );
  fd_bam_microblock_view_t bam_view[1];
  FD_TEST( fd_bam_microblock_parse( out_txn, poh_meta->sz, bam_view ) );
  FD_TEST( bam_view->txn_cnt==1UL && bam_view->result );
  fd_bam_bundle_result_t const * result = bam_view->result;
  FD_TEST( result->seq_id==101U );
  FD_TEST( result->execution_success );
  FD_TEST( result->scheduling_error==FD_BAM_SCHED_ERR_NONE );
  FD_TEST( result->feepayer_balance_lamports[0]==payer_start-fee-transfer );
  FD_TEST( test_topo_link( "bank_bam" )->mcache->sz==0UL );
  FD_TEST( env->execle->txn_out[0].err.is_committable );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_EXECUTE_SUCCESS );
  FD_TEST( out_txn->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS );
  FD_TEST( out_txn->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS );
  FD_TEST( (out_txn->flags & FD_TXN_P_FLAGS_RESULT_MASK)==0U );
  FD_TEST( test_read_lamports( env, &fee_payer )==payer_start-fee-transfer );
  FD_TEST( test_read_lamports( env, &recipient )==recipient_start+transfer );
  FD_TEST( !env->execle->txn_out[0].err.is_fees_only );

  FD_TEST( out_txn->execle_cu.actual_consumed_cus + out_txn->execle_cu.rebated_cus ==
           txn->pack_cu.non_execution_cus + txn->pack_cu.requested_exec_plus_acct_data_cus );
  FD_TEST( out_txn->execle_cu.actual_consumed_cus >= txn->pack_cu.non_execution_cus );

  fd_microblock_trailer_t const * trailer = bam_view->trailer;
  FD_TEST( trailer->pack_txn_idx==17UL );
  FD_TEST( trailer->tips==0UL );
  fd_txn_p_t txn_copy = *out_txn;
  uchar expected_hash[32];
  test_compute_expected_hash( &txn_copy, 1UL, expected_hash );
  FD_TEST( !memcmp( trailer->hash, expected_hash, 32UL ) );
  test_assert_txn_ns_dt_ordered( &trailer->txn_ns_dt );

  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_LANDED_SUCCESS_IDX ]==1UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_SUCCESS_IDX ]==1UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_simple_fee_payer_fail ) {
  /* Transaction failed */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );

  fd_pubkey_t missing_fee_payer = { .ul = { 0x3333UL } };
  fd_pubkey_t data_acct         = { .ul = { 0x4444UL } };
  ulong const data_acct_start = 777UL;
  test_fund_account( env, &data_acct, data_acct_start );

  fd_txn_p_t txn[1];
  test_build_empty_txn( txn, bank, missing_fee_payer, data_acct, 12UL, 0 );
  test_mark_bam_batch( txn, 1UL, 102U, 0 );
  test_execle_run( env, txn, 1UL, 4U, 18UL, 0 );

  test_assert_nonbundle_out( env, 1UL, 4U );
  fd_frag_meta_t const * bam_meta = test_topo_link( "bank_bam" )->mcache;
  FD_TEST( fd_frag_meta_seq_query( bam_meta )==0UL );
  FD_TEST( bam_meta->sz==sizeof(fd_bam_bundle_result_t) );
  fd_bam_bundle_result_t const * result = fd_chunk_to_laddr( env->execle->out_bam->mem, bam_meta->chunk );
  FD_TEST( result->seq_id==102U );
  FD_TEST( !result->execution_success );
  fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 0UL )->chunk );
  FD_TEST( !env->execle->txn_out[0].err.is_committable );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_TXN_ERR_ACCOUNT_NOT_FOUND );
  FD_TEST( !(out_txn->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS) );
  FD_TEST( !(out_txn->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS) );
  FD_TEST( (out_txn->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_ACCOUNT_NOT_FOUND)<<24) );
  FD_TEST( test_read_lamports( env, &data_acct )==data_acct_start );

  FD_TEST( out_txn->execle_cu.actual_consumed_cus==0U );
  FD_TEST( out_txn->execle_cu.rebated_cus==
           txn->pack_cu.non_execution_cus + txn->pack_cu.requested_exec_plus_acct_data_cus );

  fd_microblock_trailer_t const * trailer = test_out_poh_trailer_nonbundle( env, 1UL );
  FD_TEST( trailer->pack_txn_idx==18UL );
  FD_TEST( trailer->tips==0UL );
  /* hash not checked: empty bmtree (no EXECUTE_SUCCESS txns) */
  FD_TEST( trailer->txn_ns_dt.load_start  ==0.f );
  FD_TEST( trailer->txn_ns_dt.check_start ==0.f );
  FD_TEST( trailer->txn_ns_dt.exec_start  ==0.f );
  FD_TEST( trailer->txn_ns_dt.commit_start==0.f );
  FD_TEST( trailer->txn_ns_dt.commit_end  ==0.f );

  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_UNLANDED_IDX ]==1UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_ACCOUNT_NOT_FOUND_IDX ]==1UL );

  test_env_destroy( env );
}

/* Test for relax_fee_payer_constraint feature: we want to drop no-op
   transactions in the leader pipeline, as they do not charge any fees
   and so it is not profitable to include them. */
FD_UNIT_TEST( execle_simple_fee_payer_fail_relaxed ) {
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
  FD_FEATURE_SET_ACTIVE( &bank->f.features, relax_fee_payer_constraint, 0UL );

  fd_pubkey_t missing_fee_payer = { .ul = { 0x3333UL } };
  fd_pubkey_t data_acct         = { .ul = { 0x4444UL } };
  ulong const data_acct_start = 777UL;
  test_fund_account( env, &data_acct, data_acct_start );

  fd_txn_p_t txn[1];
  test_build_empty_txn( txn, bank, missing_fee_payer, data_acct, 12UL, 0 );
  test_execle_run( env, txn, 1UL, 4U, 18UL, 0 );

  test_assert_nonbundle_out( env, 1UL, 4U );
  fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 0UL )->chunk );
  FD_TEST( env->execle->txn_out[0].err.is_noop );

  /* The execle tile should have set is_committable to false */
  FD_TEST( !env->execle->txn_out[0].err.is_committable );
  FD_TEST( !env->execle->txn_out[0].err.is_fees_only );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_TXN_ERR_ACCOUNT_NOT_FOUND );
  FD_TEST( !(out_txn->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS) );
  FD_TEST( !(out_txn->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS) );
  FD_TEST( (out_txn->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_ACCOUNT_NOT_FOUND)<<24) );

  /* The transaction was dropped, so nothing was charged
     and everything is rebated. */
  FD_TEST( test_read_lamports( env, &data_acct )==data_acct_start );
  FD_TEST( !bank->f.txn_count       );
  FD_TEST( !bank->f.signature_count );
  FD_TEST( fd_bank_cost_tracker_query( bank )->block_cost==0UL );
  FD_TEST( out_txn->execle_cu.actual_consumed_cus==0U );
  FD_TEST( out_txn->execle_cu.rebated_cus==
           txn->pack_cu.non_execution_cus + txn->pack_cu.requested_exec_plus_acct_data_cus );

  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_UNLANDED_IDX ]==1UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_ACCOUNT_NOT_FOUND_IDX ]==1UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_simple_error ) {
  /* System transfer fails during execution */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );

  fd_pubkey_t fee_payer = { .ul = { 0x5151UL } };
  fd_pubkey_t recipient = { .ul = { 0x6262UL } };
  ulong const payer_start     = 1000000UL;
  ulong const recipient_start = 1234UL;
  ulong const fee             = 5000UL;
  ulong const transfer        = payer_start;

  fd_blockhash_info_t * blockhash_info = (fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue );
  FD_TEST( blockhash_info );
  blockhash_info->lamports_per_signature = fee;

  test_fund_account( env, &fee_payer, payer_start );
  test_fund_account( env, &recipient, recipient_start );

  fd_txn_p_t txn[1];
  test_build_system_transfer_txn( txn, bank, fee_payer, recipient, transfer );
  test_execle_run( env, txn, 1UL, 5U, 19UL, 0 );

  test_assert_nonbundle_out( env, 1UL, 5U );
  fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 0UL )->chunk );
  FD_TEST( env->execle->txn_out[0].err.is_committable );
  FD_TEST( !env->execle->txn_out[0].err.is_fees_only );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR );
  FD_TEST( (out_txn->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR)<<24) );
  FD_TEST( out_txn->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS );
  FD_TEST( out_txn->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS );
  FD_TEST( test_read_lamports( env, &fee_payer )==payer_start-fee );
  FD_TEST( test_read_lamports( env, &recipient )==recipient_start );

  FD_TEST( out_txn->execle_cu.actual_consumed_cus + out_txn->execle_cu.rebated_cus ==
           txn->pack_cu.non_execution_cus + txn->pack_cu.requested_exec_plus_acct_data_cus );
  FD_TEST( out_txn->execle_cu.actual_consumed_cus >= txn->pack_cu.non_execution_cus );

  fd_microblock_trailer_t const * trailer = test_out_poh_trailer_nonbundle( env, 1UL );
  FD_TEST( trailer->pack_txn_idx==19UL );
  FD_TEST( trailer->tips==0UL );
  fd_txn_p_t txn_copy = *out_txn;
  uchar expected_hash[32];
  test_compute_expected_hash( &txn_copy, 1UL, expected_hash );
  FD_TEST( !memcmp( trailer->hash, expected_hash, 32UL ) );
  test_assert_txn_ns_dt_ordered( &trailer->txn_ns_dt );

  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_LANDED_FAILED_IDX ]==1UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_INSTRUCTION_ERROR_IDX ]==1UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_bam_failed_txn_rollback_balance ) {
  /* The first transfer mutates the execution snapshot and the second fails.
     Only the fee is committed, so BAM must report the post-rollback balance. */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );

  fd_pubkey_t fee_payer    = { .ul = { 0x5353UL } };
  fd_pubkey_t recipient[2] = { { .ul = { 0x6464UL } }, { .ul = { 0x7575UL } } };
  ulong const payer_start  = 50000000000UL;
  ulong const fee          = 5000UL;
  ulong const transfer[2]  = { 1000000000UL, payer_start };

  fd_blockhash_info_t * blockhash_info = (fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue );
  FD_TEST( blockhash_info );
  blockhash_info->lamports_per_signature = fee;
  test_fund_account( env, &fee_payer, payer_start );

  fd_txn_p_t txn[1];
  test_build_system_transfer_txns( txn, bank, fee_payer, recipient, transfer, 2UL );
  test_mark_bam_batch( txn, 1UL, 104U, 0 );
  test_execle_run( env, txn, 1UL, 6U, 21UL, 0 );

  fd_frag_meta_t const * poh_meta = test_out_poh_meta( 0UL );
  fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, poh_meta->chunk );
  fd_bam_microblock_view_t bam_view[1];
  FD_TEST( fd_bam_microblock_parse( out_txn, poh_meta->sz, bam_view ) );
  FD_TEST( bam_view->txn_cnt==1UL && bam_view->result );
  fd_bam_bundle_result_t const * result = bam_view->result;

  FD_TEST( result->execution_success ); /* The non-revert batch committed. */
  FD_TEST( result->transaction_err[0]==bam_types_TransactionErrorReason_INSTRUCTION_ERROR );
  FD_TEST( result->feepayer_balance_lamports[0]==payer_start-fee );
  FD_TEST( env->execle->txn_out[0].err.is_committable && !env->execle->txn_out[0].err.is_fees_only );
  FD_TEST( out_txn->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS );
  FD_TEST( test_read_lamports( env, &fee_payer     )==payer_start-fee );
  FD_TEST( test_read_lamports( env, &recipient[0] )==0UL );
  FD_TEST( test_read_lamports( env, &recipient[1] )==0UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_simple_fees_only ) {
  /* Account loading fails after fees are validated */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );

  fd_pubkey_t fee_payer       = { .ul = { 0x7171UL } };
  fd_pubkey_t missing_program = { .ul = { 0x7272UL } };
  ulong const payer_start = 1000000UL;
  ulong const fee         = 5000UL;

  fd_blockhash_info_t * blockhash_info = (fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue );
  FD_TEST( blockhash_info );
  blockhash_info->lamports_per_signature = fee;

  test_fund_account( env, &fee_payer, payer_start );

  fd_txn_p_t txn[1];
  test_build_missing_program_txn( txn, bank, fee_payer, missing_program );
  test_execle_run( env, txn, 1UL, 6U, 20UL, 0 );

  test_assert_nonbundle_out( env, 1UL, 6U );
  fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 0UL )->chunk );
  FD_TEST( env->execle->txn_out[0].err.is_committable );
  FD_TEST( env->execle->txn_out[0].err.is_fees_only );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_TXN_ERR_PROGRAM_ACCOUNT_NOT_FOUND );
  FD_TEST( (out_txn->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_PROGRAM_ACCOUNT_NOT_FOUND)<<24) );
  FD_TEST( out_txn->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS );
  FD_TEST( out_txn->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS );
  FD_TEST( out_txn->execle_cu.actual_consumed_cus + out_txn->execle_cu.rebated_cus ==
           txn->pack_cu.non_execution_cus + txn->pack_cu.requested_exec_plus_acct_data_cus );
  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_LANDED_FEES_ONLY_IDX ]==1UL );
  FD_TEST( test_read_lamports( env, &fee_payer )==payer_start-fee );
  FD_TEST( out_txn->execle_cu.actual_consumed_cus >= txn->pack_cu.non_execution_cus );

  fd_microblock_trailer_t const * trailer = test_out_poh_trailer_nonbundle( env, 1UL );
  FD_TEST( trailer->pack_txn_idx==20UL );
  FD_TEST( trailer->tips==0UL );
  fd_txn_p_t txn_copy = *out_txn;
  uchar expected_hash[32];
  test_compute_expected_hash( &txn_copy, 1UL, expected_hash );
  FD_TEST( !memcmp( trailer->hash, expected_hash, 32UL ) );
  test_assert_txn_ns_dt_ordered( &trailer->txn_ns_dt );

  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_PROGRAM_ACCOUNT_NOT_FOUND_IDX ]==1UL );

  test_env_destroy( env );
}

/* Multi-transaction bundle tests.  Bundle execution forwards account
   state between txns through prev_txn_outs (the new accdb defers account
   release to commit/cancel time); see handle_bundle() in the tile and
   fd_runtime_commit_txn / fd_runtime_cancel_txn in fd_runtime.c. */
FD_UNIT_TEST( execle_bundle_ok ) {
  /* Successful bundle */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );

  fd_pubkey_t fee_payer  = { .ul = { 0x5555UL } };
  fd_pubkey_t recipient0 = { .ul = { 0x6666UL } };
  fd_pubkey_t recipient1 = { .ul = { 0x6667UL } };
  ulong const fee              = 5000UL;
  ulong const payer_start      = 1000000000UL;
  ulong const recipient0_start = 111UL;
  ulong const recipient1_start = 222UL;
  ulong const transfer0        = 1234567UL;
  ulong const transfer1        = 7654321UL;

  fd_blockhash_info_t * blockhash_info = (fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue );
  FD_TEST( blockhash_info );
  blockhash_info->lamports_per_signature = fee;

  test_fund_account( env, &fee_payer,  payer_start );
  test_fund_account( env, &recipient0, recipient0_start );
  test_fund_account( env, &recipient1, recipient1_start );

  fd_txn_p_t txns[2];
  test_build_system_transfer_txn( &txns[0], bank, fee_payer, recipient0, transfer0 );
  test_build_system_transfer_txn( &txns[1], bank, fee_payer, recipient1, transfer1 );
  test_mark_bam_batch( txns, 2UL, 103U, 1 );
  test_execle_run( env, txns, 2UL, 8U, 21UL, 1 );

  test_assert_bundle_out( env, 1UL, 8U );
  fd_bam_microblock_view_t bam_view[1];
  fd_frag_meta_t const * final_meta = test_out_poh_meta( 1UL );
  FD_TEST( fd_bam_microblock_parse( fd_chunk_to_laddr( env->execle->out_poh->mem, final_meta->chunk ), final_meta->sz, bam_view ) );
  FD_TEST( fd_frag_meta_seq_query( final_meta )==1UL );
  FD_TEST( final_meta->sig==fd_disco_execle_sig( bank->f.slot, 9U ) );
  FD_TEST( bam_view->txn_cnt==1UL && bam_view->result );
  FD_TEST( bam_view->result->seq_id==103U );
  FD_TEST( bam_view->result->execution_success );
  FD_TEST( test_topo_link( "bank_bam" )->mcache->sz==0UL );
  FD_TEST( env->execle->txn_out[0].err.is_committable );
  FD_TEST( env->execle->txn_out[1].err.is_committable );
  for( ulong i=0UL; i<2UL; i++ ) {
    fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( i )->chunk );
    FD_TEST( out_txn->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS );
    FD_TEST( out_txn->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS );
    FD_TEST( (out_txn->flags & FD_TXN_P_FLAGS_RESULT_MASK)==0U );
    FD_TEST( !env->execle->txn_out[i].err.is_fees_only );
    FD_TEST( env->execle->txn_out[i].err.txn_err==FD_RUNTIME_EXECUTE_SUCCESS );

    FD_TEST( out_txn->execle_cu.actual_consumed_cus + out_txn->execle_cu.rebated_cus ==
             txns[i].pack_cu.non_execution_cus + txns[i].pack_cu.requested_exec_plus_acct_data_cus );
    FD_TEST( out_txn->execle_cu.actual_consumed_cus >= txns[i].pack_cu.non_execution_cus );

    fd_microblock_trailer_t const * trailer = test_out_poh_trailer_bundle( env, i );
    FD_TEST( trailer->pack_txn_idx==21UL+i );
    FD_TEST( trailer->tips==0UL );
    fd_txn_p_t txn_copy = *out_txn;
    uchar expected_hash[32];
    test_compute_expected_hash( &txn_copy, 1UL, expected_hash );
    FD_TEST( !memcmp( trailer->hash, expected_hash, 32UL ) );
    test_assert_txn_ns_dt_ordered( &trailer->txn_ns_dt );
  }

  FD_TEST( test_read_lamports( env, &fee_payer  )==payer_start - 2UL*fee - transfer0 - transfer1 );
  FD_TEST( test_read_lamports( env, &recipient0 )==recipient0_start + transfer0 );
  FD_TEST( test_read_lamports( env, &recipient1 )==recipient1_start + transfer1 );

  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_LANDED_SUCCESS_IDX ]==2UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_SUCCESS_IDX ]==2UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_bundle_fail ) {
  /* Instruction in a bundle reverts the whole bundle */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );

  fd_pubkey_t fee_payer  = { .ul = { 0x7777UL } };
  fd_pubkey_t recipient0 = { .ul = { 0x8888UL } };
  fd_pubkey_t recipient1 = { .ul = { 0x9999UL } };
  ulong const fee              = 5000UL;
  ulong const payer_start      = 1000000000UL;
  ulong const recipient0_start = 111UL;
  ulong const recipient1_start = 222UL;
  ulong const first_transfer   = 100000000UL;
  ulong const second_transfer  = payer_start;

  fd_blockhash_info_t * blockhash_info = (fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue );
  FD_TEST( blockhash_info );
  blockhash_info->lamports_per_signature = fee;

  test_fund_account( env, &fee_payer,  payer_start );
  test_fund_account( env, &recipient0, recipient0_start );
  test_fund_account( env, &recipient1, recipient1_start );

  fd_txn_p_t txns[2];
  test_build_system_transfer_txn( &txns[0], bank, fee_payer, recipient0, first_transfer );
  test_build_system_transfer_txn( &txns[1], bank, fee_payer, recipient1, second_transfer );
  test_execle_run( env, txns, 2UL, 12U, 31UL, 1 );

  test_assert_bundle_out( env, 2UL, 12U );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_EXECUTE_SUCCESS );
  FD_TEST( env->execle->txn_out[1].err.txn_err==FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR );
  for( ulong i=0UL; i<2UL; i++ ) {
    fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( i )->chunk );
    FD_TEST( !env->execle->txn_out[i].err.is_committable );
    FD_TEST( !(out_txn->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS) );
    FD_TEST( !(out_txn->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS) );

    FD_TEST( out_txn->execle_cu.actual_consumed_cus==0U );
    FD_TEST( out_txn->execle_cu.rebated_cus==
             txns[i].pack_cu.requested_exec_plus_acct_data_cus + txns[i].pack_cu.non_execution_cus );

    fd_microblock_trailer_t const * trailer = test_out_poh_trailer_bundle( env, i );
    FD_TEST( trailer->pack_txn_idx==31UL+i );
    FD_TEST( trailer->tips==0UL );
    /* hash not checked: empty bmtree (no EXECUTE_SUCCESS txns) */
  }

  fd_txn_p_t const * out_txn0 = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 0UL )->chunk );
  fd_txn_p_t const * out_txn1 = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 1UL )->chunk );
  FD_TEST( (out_txn0->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_BUNDLE_PEER)<<24) );
  FD_TEST( (out_txn1->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR)<<24) );

  test_execle_flush_rebate( env );
  fd_frag_meta_t const * rebate_meta = test_out_pack_meta( 0UL );
  FD_TEST( fd_frag_meta_seq_query( rebate_meta )==0UL );
  FD_TEST( rebate_meta->sig==bank->f.slot );
  FD_TEST( rebate_meta->sz>=FD_PACK_REBATE_MIN_SZ );
  fd_pack_rebate_t const * rebate = fd_chunk_to_laddr( env->execle->out_pack->mem, rebate_meta->chunk );
  ulong expected_rebated_cus = 0UL;
  ulong expected_data_bytes  = 2UL*48UL;
  for( ulong i=0UL; i<2UL; i++ ) {
    expected_rebated_cus += txns[i].pack_cu.requested_exec_plus_acct_data_cus + txns[i].pack_cu.non_execution_cus;
    expected_data_bytes  += txns[i].payload_sz;
  }
  FD_TEST( rebate->total_cost_rebate    ==expected_rebated_cus );
  FD_TEST( rebate->data_bytes_rebate    ==expected_data_bytes  );
  FD_TEST( rebate->microblock_cnt_rebate==2UL                  );
  FD_TEST( rebate->alloc_rebate         ==0UL                  );
  FD_TEST( rebate->ib_result            ==0                    );

  FD_TEST( test_read_lamports( env, &fee_payer  )==payer_start      );
  FD_TEST( test_read_lamports( env, &recipient0 )==recipient0_start );
  FD_TEST( test_read_lamports( env, &recipient1 )==recipient1_start );

  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_UNLANDED_IDX ]==2UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_INSTRUCTION_ERROR_IDX ]==1UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_BUNDLE_PEER_IDX ]==1UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_bundle_peer_fail ) {
  /* A middle transaction failure skips the rest of the bundle. */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );

  fd_pubkey_t fee_payer  = { .ul = { 0x7979UL } };
  fd_pubkey_t recipient0 = { .ul = { 0x8989UL } };
  fd_pubkey_t recipient1 = { .ul = { 0x9998UL } };
  fd_pubkey_t recipient2 = { .ul = { 0xa9a9UL } };
  ulong const fee              = 5000UL;
  ulong const payer_start      = 1000000000UL;
  ulong const recipient0_start = 111UL;
  ulong const recipient1_start = 222UL;
  ulong const recipient2_start = 333UL;
  ulong const first_transfer   = 100000000UL;
  ulong const second_transfer  = payer_start;
  ulong const third_transfer   = 1UL;

  fd_blockhash_info_t * blockhash_info = (fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue );
  FD_TEST( blockhash_info );
  blockhash_info->lamports_per_signature = fee;

  test_fund_account( env, &fee_payer,  payer_start );
  test_fund_account( env, &recipient0, recipient0_start );
  test_fund_account( env, &recipient1, recipient1_start );
  test_fund_account( env, &recipient2, recipient2_start );

  fd_txn_p_t txns[3];
  test_build_system_transfer_txn( &txns[0], bank, fee_payer, recipient0, first_transfer  );
  test_build_system_transfer_txn( &txns[1], bank, fee_payer, recipient1, second_transfer );
  test_build_system_transfer_txn( &txns[2], bank, fee_payer, recipient2, third_transfer  );
  test_execle_run( env, txns, 3UL, 14U, 35UL, 1 );

  test_assert_bundle_out( env, 3UL, 14U );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_EXECUTE_SUCCESS );
  FD_TEST( env->execle->txn_out[1].err.txn_err==FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR );
  FD_TEST( env->execle->txn_out[2].err.txn_err==FD_RUNTIME_EXECUTE_SUCCESS );
  FD_TEST( env->execle->txn_out[2].details.load_start_ticks  ==LONG_MAX );
  FD_TEST( env->execle->txn_out[2].details.check_start_ticks ==LONG_MAX );
  FD_TEST( env->execle->txn_out[2].details.exec_start_ticks  ==LONG_MAX );
  FD_TEST( env->execle->txn_out[2].details.commit_start_ticks==LONG_MAX );

  for( ulong i=0UL; i<3UL; i++ ) {
    fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( i )->chunk );
    FD_TEST( !env->execle->txn_out[i].err.is_committable );
    FD_TEST( !(out_txn->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS) );
    FD_TEST( !(out_txn->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS) );

    FD_TEST( out_txn->execle_cu.actual_consumed_cus==0U );
    FD_TEST( out_txn->execle_cu.rebated_cus==
             txns[i].pack_cu.requested_exec_plus_acct_data_cus + txns[i].pack_cu.non_execution_cus );

    fd_microblock_trailer_t const * trailer = test_out_poh_trailer_bundle( env, i );
    FD_TEST( trailer->pack_txn_idx==35UL+i );
    FD_TEST( trailer->tips==0UL );
    /* hash not checked: empty bmtree (no EXECUTE_SUCCESS txns) */
  }

  /* Txns 0 and 1 were executed, ordering invariant holds */
  for( ulong i=0UL; i<2UL; i++ ) {
    fd_microblock_trailer_t const * trailer = test_out_poh_trailer_bundle( env, i );
    test_assert_txn_ns_dt_ordered( &trailer->txn_ns_dt );
  }
  /* Txn 2 was never executed, all zeros */
  {
    fd_microblock_trailer_t const * trailer = test_out_poh_trailer_bundle( env, 2UL );
    FD_TEST( trailer->txn_ns_dt.load_start  ==0.f );
    FD_TEST( trailer->txn_ns_dt.check_start ==0.f );
    FD_TEST( trailer->txn_ns_dt.exec_start  ==0.f );
    FD_TEST( trailer->txn_ns_dt.commit_start==0.f );
    FD_TEST( trailer->txn_ns_dt.commit_end  ==0.f );
  }

  fd_txn_p_t const * out_txn0 = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 0UL )->chunk );
  fd_txn_p_t const * out_txn1 = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 1UL )->chunk );
  fd_txn_p_t const * out_txn2 = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 2UL )->chunk );
  FD_TEST( (out_txn0->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_BUNDLE_PEER)<<24) );
  FD_TEST( (out_txn1->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR)<<24) );
  FD_TEST( (out_txn2->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_BUNDLE_PEER)<<24) );
  FD_TEST( out_txn0->flags & FD_TXN_P_FLAGS_BUNDLE );
  FD_TEST( out_txn1->flags & FD_TXN_P_FLAGS_BUNDLE );
  FD_TEST( out_txn2->flags & FD_TXN_P_FLAGS_BUNDLE );

  test_execle_flush_rebate( env );
  fd_frag_meta_t const * rebate_meta = test_out_pack_meta( 0UL );
  FD_TEST( fd_frag_meta_seq_query( rebate_meta )==0UL );
  FD_TEST( rebate_meta->sig==bank->f.slot );
  FD_TEST( rebate_meta->sz>=FD_PACK_REBATE_MIN_SZ );
  fd_pack_rebate_t const * rebate = fd_chunk_to_laddr( env->execle->out_pack->mem, rebate_meta->chunk );
  ulong expected_rebated_cus = 0UL;
  ulong expected_data_bytes  = 3UL*48UL;
  for( ulong i=0UL; i<3UL; i++ ) {
    expected_rebated_cus += txns[i].pack_cu.requested_exec_plus_acct_data_cus + txns[i].pack_cu.non_execution_cus;
    expected_data_bytes  += txns[i].payload_sz;
  }
  FD_TEST( rebate->total_cost_rebate    ==expected_rebated_cus );
  FD_TEST( rebate->data_bytes_rebate    ==expected_data_bytes  );
  FD_TEST( rebate->microblock_cnt_rebate==3UL                 );
  FD_TEST( rebate->alloc_rebate         ==0UL                 );
  FD_TEST( rebate->ib_result            ==0                   );

  FD_TEST( test_read_lamports( env, &fee_payer  )==payer_start      );
  FD_TEST( test_read_lamports( env, &recipient0 )==recipient0_start );
  FD_TEST( test_read_lamports( env, &recipient1 )==recipient1_start );
  FD_TEST( test_read_lamports( env, &recipient2 )==recipient2_start );

  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_UNLANDED_IDX ]==3UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_INSTRUCTION_ERROR_IDX ]==1UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_BUNDLE_PEER_IDX ]==2UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_bundle_progcache ) {
  /* Close a deployed/rooted program in the first transaction,
     then try to invoke it in the second transaction (should fail). */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );

  fd_pubkey_t authority   = { .ul = { 0xaaa0UL } };
  fd_pubkey_t recipient   = { .ul = { 0xaaa1UL } };
  fd_pubkey_t program     = { .ul = { 0xaaa3UL } };
  fd_pubkey_t programdata = { .ul = { 0xaaa4UL } };

  ulong const authority_start   = 1000000000UL;
  ulong const recipient_start   = 123UL;
  ulong const program_lamports  = 1000000UL;
  ulong const programdata_lamports = 2000000UL;

  test_fund_account( env, &authority,  authority_start  );
  test_fund_account( env, &recipient,  recipient_start  );

  uchar program_state_data[ SIZE_OF_PROGRAM ];
  fd_bpf_state_t program_state = {
    .discriminant = FD_BPF_STATE_PROGRAM,
    .inner.program.programdata_address = programdata
  };
  ulong out_sz = 0UL;
  FD_TEST( !fd_bpf_state_encode( &program_state, program_state_data, sizeof(program_state_data), &out_sz ) );

  uchar programdata_state_data[ PROGRAMDATA_METADATA_SIZE + test_bpf_program_sz ];
  fd_bpf_state_t programdata_state = {
    .discriminant = FD_BPF_STATE_PROGRAM_DATA,
    .inner.program_data = {
      .slot = bank->f.slot - 1UL,
      .upgrade_authority_address = authority,
      .has_upgrade_authority_address = 1
    }
  };
  out_sz = 0UL;
  FD_TEST( !fd_bpf_state_encode( &programdata_state, programdata_state_data, PROGRAMDATA_METADATA_SIZE, &out_sz ) );
  fd_memcpy( programdata_state_data + PROGRAMDATA_METADATA_SIZE, test_bpf_program, test_bpf_program_sz );

  test_put_account_rooted( env, &program, &fd_solana_bpf_loader_upgradeable_program_id,
                           program_lamports, bank->f.slot-1UL, 1, program_state_data, sizeof(program_state_data) );
  test_put_account_rooted( env, &programdata, &fd_solana_bpf_loader_upgradeable_program_id,
                           programdata_lamports, bank->f.slot-1UL, 0,
                           programdata_state_data, sizeof(programdata_state_data) );

  fd_txn_p_t txns[2];
  test_build_bpf_close_txn( &txns[0], bank, authority, recipient, program, programdata );
  test_build_program_invoke_txn( &txns[1], bank, authority, program );
  test_execle_run( env, txns, 2UL, 16U, 41UL, 1 );

  test_assert_bundle_out( env, 2UL, 16U );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_EXECUTE_SUCCESS );
  FD_TEST( env->execle->txn_out[1].err.txn_err==FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR );
  for( ulong i=0UL; i<2UL; i++ ) {
    fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( i )->chunk );
    FD_TEST( !env->execle->txn_out[i].err.is_committable );
    FD_TEST( !(out_txn->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS) );
    FD_TEST( !(out_txn->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS) );

    FD_TEST( out_txn->execle_cu.actual_consumed_cus==0U );
    FD_TEST( out_txn->execle_cu.rebated_cus==
             txns[i].pack_cu.requested_exec_plus_acct_data_cus + txns[i].pack_cu.non_execution_cus );

    fd_microblock_trailer_t const * trailer = test_out_poh_trailer_bundle( env, i );
    FD_TEST( trailer->pack_txn_idx==41UL+i );
    FD_TEST( trailer->tips==0UL );
    /* hash not checked: empty bmtree (no EXECUTE_SUCCESS txns) */
    test_assert_txn_ns_dt_ordered( &trailer->txn_ns_dt );
  }
  FD_TEST( test_read_lamports( env, &authority   )==authority_start   );
  FD_TEST( test_read_lamports( env, &recipient   )==recipient_start   );
  FD_TEST( test_read_lamports( env, &program     )==program_lamports  );
  FD_TEST( test_read_lamports( env, &programdata )==programdata_lamports );

  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_UNLANDED_IDX ]==2UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_INSTRUCTION_ERROR_IDX ]==1UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_BUNDLE_PEER_IDX ]==1UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_bundle_nonce_dup ) {
  /* Advance the same durable nonce twice */
  test_env_t * env = test_env_create();

  fd_pubkey_t fee_payer  = { .ul = { 0xccc0UL } };
  fd_pubkey_t nonce_key  = { .ul = { 0xccc1UL } };
  fd_pubkey_t recipient0 = { .ul = { 0xccc2UL } };
  fd_pubkey_t recipient1 = { .ul = { 0xccc3UL } };

  ulong const fee_payer_start  = 10000000000UL;
  ulong const nonce_start      = 10000000000UL;
  ulong const recipient0_start = 1000000000UL;
  ulong const recipient1_start = 1000000000UL;
  ulong const transfer0        = 12345UL;
  ulong const transfer1        = 67890UL;
  ulong const fee              = 5000UL;

  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
  fd_blockhash_info_t * blockhash_info = (fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue );
  FD_TEST( blockhash_info );
  blockhash_info->lamports_per_signature = fee;

  fd_hash_t stale_blockhash = {0};
  fd_memset( stale_blockhash.uc, 0x42, sizeof(fd_hash_t) );
  fd_hash_t durable_nonce;
  test_durable_nonce_from_blockhash( &durable_nonce, &stale_blockhash );

  test_fund_account( env, &fee_payer,  fee_payer_start  );
  test_fund_account( env, &recipient0, recipient0_start );
  test_fund_account( env, &recipient1, recipient1_start );
  test_put_nonce_account_rooted( env, &nonce_key, &fee_payer, &durable_nonce, nonce_start );

  fd_txn_p_t txns[2];
  test_build_durable_nonce_transfer_txn( &txns[0], fee_payer, nonce_key, recipient0, &durable_nonce, transfer0, 51UL );
  test_build_durable_nonce_transfer_txn( &txns[1], fee_payer, nonce_key, recipient1, &durable_nonce, transfer1, 52UL );
  test_execle_run( env, txns, 2UL, 20U, 51UL, 1 );

  test_assert_bundle_out( env, 2UL, 20U );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_EXECUTE_SUCCESS );
  FD_TEST( env->execle->txn_out[1].err.txn_err==FD_RUNTIME_TXN_ERR_BLOCKHASH_FAIL_WRONG_NONCE );

  fd_txn_p_t const * out_txn0 = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 0UL )->chunk );
  fd_txn_p_t const * out_txn1 = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 1UL )->chunk );
  FD_TEST( !env->execle->txn_out[0].err.is_committable );
  FD_TEST( !env->execle->txn_out[1].err.is_committable );
  FD_TEST( !(out_txn0->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS) );
  FD_TEST( !(out_txn0->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS) );
  FD_TEST( !(out_txn1->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS) );
  FD_TEST( !(out_txn1->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS) );
  FD_TEST( out_txn0->flags & FD_TXN_P_FLAGS_DURABLE_NONCE );
  FD_TEST( out_txn1->flags & FD_TXN_P_FLAGS_DURABLE_NONCE );
  FD_TEST( (out_txn0->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_BUNDLE_PEER)<<24) );
  FD_TEST( (out_txn1->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_BLOCKHASH_FAIL_WRONG_NONCE)<<24) );

  for( ulong i=0UL; i<2UL; i++ ) {
    fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( i )->chunk );
    FD_TEST( out_txn->execle_cu.actual_consumed_cus==0U );
    FD_TEST( out_txn->execle_cu.rebated_cus==
             txns[i].pack_cu.requested_exec_plus_acct_data_cus + txns[i].pack_cu.non_execution_cus );

    fd_microblock_trailer_t const * trailer = test_out_poh_trailer_bundle( env, i );
    FD_TEST( trailer->pack_txn_idx==51UL+i );
    FD_TEST( trailer->tips==0UL );
    /* hash not checked: empty bmtree (no EXECUTE_SUCCESS txns) */
    test_assert_txn_ns_dt_ordered( &trailer->txn_ns_dt );
  }

  FD_TEST( test_read_lamports( env, &fee_payer  )==fee_payer_start  );
  FD_TEST( test_read_lamports( env, &nonce_key  )==nonce_start      );
  FD_TEST( test_read_lamports( env, &recipient0 )==recipient0_start );
  FD_TEST( test_read_lamports( env, &recipient1 )==recipient1_start );

  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_UNLANDED_IDX ]==2UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_NONCE_WRONG_BLOCKHASH_IDX ]==1UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_BUNDLE_PEER_IDX ]==1UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_bundle_nonce_dup2 ) {
  /* Advance the same nonce account twice with valid durable nonces */
  test_env_t * env = test_env_create();

  fd_pubkey_t fee_payer  = { .ul = { 0xcce0UL } };
  fd_pubkey_t nonce_key  = { .ul = { 0xcce1UL } };
  fd_pubkey_t recipient0 = { .ul = { 0xcce2UL } };
  fd_pubkey_t recipient1 = { .ul = { 0xcce3UL } };

  ulong const fee_payer_start  = 10000000000UL;
  ulong const nonce_start      = 10000000000UL;
  ulong const recipient0_start = 1000000000UL;
  ulong const recipient1_start = 1000000000UL;
  ulong const transfer0        = 12345UL;
  ulong const transfer1        = 67890UL;
  ulong const fee              = 5000UL;

  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
  fd_blockhash_info_t * blockhash_info = (fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue );
  FD_TEST( blockhash_info );
  blockhash_info->lamports_per_signature = fee;

  fd_hash_t stale_blockhash = {0};
  fd_memset( stale_blockhash.uc, 0x43, sizeof(fd_hash_t) );
  fd_hash_t durable_nonce0;
  test_durable_nonce_from_blockhash( &durable_nonce0, &stale_blockhash );

  fd_hash_t const * last_blockhash = fd_blockhashes_peek_last_hash( &bank->f.block_hash_queue );
  FD_TEST( last_blockhash );
  fd_hash_t durable_nonce1;
  test_durable_nonce_from_blockhash( &durable_nonce1, last_blockhash );

  test_fund_account( env, &fee_payer,  fee_payer_start  );
  test_fund_account( env, &recipient0, recipient0_start );
  test_fund_account( env, &recipient1, recipient1_start );
  test_put_nonce_account_rooted( env, &nonce_key, &fee_payer, &durable_nonce0, nonce_start );

  fd_txn_p_t txns[2];
  test_build_durable_nonce_transfer_txn( &txns[0], fee_payer, nonce_key, recipient0, &durable_nonce0, transfer0, 53UL );
  test_build_durable_nonce_transfer_txn( &txns[1], fee_payer, nonce_key, recipient1, &durable_nonce1, transfer1, 54UL );
  test_execle_run( env, txns, 2UL, 22U, 53UL, 1 );

  test_assert_bundle_out( env, 2UL, 22U );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_EXECUTE_SUCCESS );
  FD_TEST( env->execle->txn_out[1].err.txn_err==FD_RUNTIME_TXN_ERR_BLOCKHASH_NONCE_ALREADY_ADVANCED );

  fd_txn_p_t const * out_txn0 = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 0UL )->chunk );
  fd_txn_p_t const * out_txn1 = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 1UL )->chunk );
  FD_TEST( !env->execle->txn_out[0].err.is_committable );
  FD_TEST( !env->execle->txn_out[1].err.is_committable );
  FD_TEST( !(out_txn0->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS) );
  FD_TEST( !(out_txn0->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS) );
  FD_TEST( !(out_txn1->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS) );
  FD_TEST( !(out_txn1->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS) );
  FD_TEST( out_txn0->flags & FD_TXN_P_FLAGS_DURABLE_NONCE );
  FD_TEST( out_txn1->flags & FD_TXN_P_FLAGS_DURABLE_NONCE );
  FD_TEST( (out_txn0->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_BUNDLE_PEER)<<24) );
  FD_TEST( (out_txn1->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_BLOCKHASH_NONCE_ALREADY_ADVANCED)<<24) );

  for( ulong i=0UL; i<2UL; i++ ) {
    fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( i )->chunk );
    FD_TEST( out_txn->execle_cu.actual_consumed_cus==0U );
    FD_TEST( out_txn->execle_cu.rebated_cus==
             txns[i].pack_cu.requested_exec_plus_acct_data_cus + txns[i].pack_cu.non_execution_cus );

    fd_microblock_trailer_t const * trailer = test_out_poh_trailer_bundle( env, i );
    FD_TEST( trailer->pack_txn_idx==53UL+i );
    FD_TEST( trailer->tips==0UL );
    /* hash not checked: empty bmtree (no EXECUTE_SUCCESS txns) */
    test_assert_txn_ns_dt_ordered( &trailer->txn_ns_dt );
  }

  FD_TEST( test_read_lamports( env, &fee_payer  )==fee_payer_start  );
  FD_TEST( test_read_lamports( env, &nonce_key  )==nonce_start      );
  FD_TEST( test_read_lamports( env, &recipient0 )==recipient0_start );
  FD_TEST( test_read_lamports( env, &recipient1 )==recipient1_start );

  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_UNLANDED_IDX ]==2UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_NONCE_ALREADY_ADVANCED_IDX ]==1UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_BUNDLE_PEER_IDX ]==1UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_bundle_dup ) {
  /* Duplicate transaction in a bundle causing a status cache collision */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );

  fd_pubkey_t fee_payer = { .ul = { 0xeeeeUL } };
  fd_pubkey_t shared    = { .ul = { 0xffffUL } };
  test_fund_account( env, &fee_payer, 1000000000UL );

  fd_txn_p_t txns[2];
  test_build_empty_txn( &txns[0], bank, fee_payer, shared, 61UL, 0 );
  txns[1] = txns[0];
  test_execle_run( env, txns, 2UL, 24U, 61UL, 1 );

  test_assert_bundle_out( env, 2UL, 24U );
  FD_TEST( !env->execle->txn_out[0].err.is_committable );
  FD_TEST( !env->execle->txn_out[1].err.is_committable );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_EXECUTE_SUCCESS );
  FD_TEST( env->execle->txn_out[1].err.txn_err==FD_RUNTIME_TXN_ERR_ALREADY_PROCESSED );

  fd_txn_p_t const * out_txn0 = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 0UL )->chunk );
  fd_txn_p_t const * out_txn1 = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( 1UL )->chunk );
  FD_TEST( !(out_txn0->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS) );
  FD_TEST( !(out_txn0->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS) );
  FD_TEST( !(out_txn1->flags & FD_TXN_P_FLAGS_SANITIZE_SUCCESS) );
  FD_TEST( !(out_txn1->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS) );
  FD_TEST( (out_txn0->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_BUNDLE_PEER)<<24) );
  FD_TEST( (out_txn1->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)(-FD_RUNTIME_TXN_ERR_ALREADY_PROCESSED)<<24) );

  for( ulong i=0UL; i<2UL; i++ ) {
    fd_txn_p_t const * out_txn = fd_chunk_to_laddr( env->execle->out_poh->mem, test_out_poh_meta( i )->chunk );
    FD_TEST( out_txn->execle_cu.actual_consumed_cus==0U );
    FD_TEST( out_txn->execle_cu.rebated_cus==
             txns[i].pack_cu.requested_exec_plus_acct_data_cus + txns[i].pack_cu.non_execution_cus );

    fd_microblock_trailer_t const * trailer = test_out_poh_trailer_bundle( env, i );
    FD_TEST( trailer->pack_txn_idx==61UL+i );
    FD_TEST( trailer->tips==0UL );
    /* hash not checked: empty bmtree (no EXECUTE_SUCCESS txns) */
    test_assert_txn_ns_dt_ordered( &trailer->txn_ns_dt );
  }

  FD_TEST( env->execle->metrics.txn_landed[ FD_METRICS_ENUM_TRANSACTION_LANDED_V_UNLANDED_IDX ]==2UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_ALREADY_PROCESSED_IDX ]==1UL );
  FD_TEST( env->execle->metrics.txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_V_BUNDLE_PEER_IDX ]==1UL );

  test_env_destroy( env );
}

static char const * test_bam_fixture_path;

typedef struct {
  fd_stem_context_t stem[1];
  fd_frag_meta_t * mcaches[3];
  ulong seqs[3];
  ulong depths[3];
  ulong credits[3];
  ulong min_credit;
  int reliable[3];
} test_bam_worker_output_t;

static void
test_bam_worker_output_init( test_bam_worker_output_t * out,
                             ulong                      worker ) {
  fd_memset( out, 0, sizeof(*out) );
  char const * names[3] = { "execle_poh", "execle_pack", "bank_bam" };
  for( ulong i=0UL; i<3UL; i++ ) {
    fd_topo_link_t const * link = test_topo_link_kind( names[i], worker );
    out->mcaches[i] = link->mcache;
    out->depths[i] = link->depth;
    out->credits[i] = link->depth;
    out->reliable[i] = 1;
  }
  out->min_credit = out->depths[0];
  *out->stem = (fd_stem_context_t){ .mcaches=out->mcaches, .seqs=out->seqs, .depths=out->depths,
      .cr_avail=out->credits, .min_cr_avail=&out->min_credit, .cr_decrement_amount=1UL,
      .out_reliable=out->reliable };
}

static void
test_bam_execute_pack_output( test_env_t *             env,
                              test_bam_worker_output_t * out,
                              fd_txn_e_t const *         txns,
                              ulong                      txn_cnt,
                              uint                       pack_idx,
                              ulong                      pack_txn_idx ) {
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
  ulong chunk = env->execle->pack_in_chunk0;
  fd_txn_e_t * in = fd_chunk_to_laddr( env->execle->pack_in_mem, chunk );
  fd_memcpy( in, txns, txn_cnt*sizeof(fd_txn_e_t) );
  fd_microblock_execle_trailer_t * trailer = (fd_microblock_execle_trailer_t *)(in+txn_cnt);
  *trailer = (fd_microblock_execle_trailer_t){ .bank_idx=env->bank_idx,
      .pack_idx=pack_idx, .pack_txn_idx=pack_txn_idx,
      .is_bundle=!!(txns[0].txnp->flags & FD_TXN_P_FLAGS_BUNDLE) };
  ulong sig = fd_disco_poh_sig( bank->f.slot, POH_PKT_TYPE_MICROBLOCK, env->execle->kind_id );
  ulong sz = txn_cnt*sizeof(fd_txn_e_t)+sizeof(*trailer);
  FD_TEST( !before_frag( env->execle, 0UL, 0UL, sig ) );
  during_frag( env->execle, 0UL, 0UL, sig, chunk, sz, 0UL );
  after_frag( env->execle, 0UL, 0UL, sig, sz, 0UL, fd_frag_meta_ts_comp( fd_tickcount() ), out->stem );
  FD_TEST( fd_fseq_query( env->execle->busy_fseq )==0UL );
  /* Current execle batches CU rebates.  Flush through its credited
     callback so the benchmark observes the same feedback as Pack. */
  if( env->execle->rebate_microblock_cnt ) {
    int poll=1, busy=0;
    env->execle->rebate_idle_loop_cnt = REBATE_BATCH_IDLE_LOOPS;
    after_credit( env->execle, out->stem, &poll, &busy );
    FD_TEST( busy );
  }
}

static int
test_bam_poll_poh( test_bam_poh_fixture_t * poh,
                    test_env_t const *     env,
                    test_bam_worker_output_t const * out,
                    ulong                  seq ) {
  fd_frag_meta_t const * m = out->mcaches[0]+fd_mcache_line_idx( seq, out->depths[0] );
  FD_TEST( fd_frag_meta_seq_query( m )==seq );
  return test_bam_poh_fixture_consume( poh, env->execle->kind_id, m->sig,
      fd_chunk_to_laddr_const( env->execle->out_poh->mem, m->chunk ), m->sz );
}

static void
test_bam_pair_sign( fd_txn_p_t *       txnp,
                     fd_pubkey_t const * signer,
                     uchar const         private_key[32] ) {
  fd_sha512_t sha[1];
  FD_TEST( fd_sha512_join( fd_sha512_new( sha ) ) );
  fd_txn_t const * txn = TXN(txnp);
  uchar * signature = txnp->payload+txn->signature_off;
  uchar const * message = txnp->payload+txn->message_off;
  ulong message_sz = fd_txn_msg_sz( txn, txnp->payload_sz );
  fd_ed25519_sign( signature, message, message_sz, signer->uc, private_key, sha );
  FD_TEST( fd_ed25519_verify( message, message_sz, signature, signer->uc, sha )==FD_ED25519_SUCCESS );
  fd_sha512_delete( fd_sha512_leave( sha ) );
}

typedef struct {
  ulong target_slot;
  ulong slot;
  ulong first_batch_cnt;
  ulong second_batch_cnt;
  ulong forwarded_cnt;
  int atomic;
  fd_txn_p_t forwarded[4];
  test_bam_poh_summary_t poh;
  fd_bam_bundle_result_t terminal[2];
  ulong balances[5];
} test_bam_pair_result_t;

static void
test_bam_pair_insert( fd_pack_t * pack,
                       fd_txn_p_t const * txns,
                       ulong count,
                       ulong slot ) {
  fd_txn_e_t * slots[FD_PACK_MAX_TXN_PER_BUNDLE];
  fd_pack_insert_bundle_init( pack, slots, count );
  for( ulong i=0UL; i<count; i++ ) {
    fd_memset( slots[i], 0, sizeof(fd_txn_e_t) );
    *slots[i]->txnp = txns[i];
  }
  ulong deleted;
  FD_TEST( fd_pack_insert_bundle_fini( pack, slots, count, slot, FD_PACK_IB_TYPE_NONE,
                                       NULL, &deleted, NULL )>=0 );
  FD_TEST( !deleted );
}

/* Compare serial execution with reverse completion on independent workers.
   Pack owns scheduling and locks; PoH independently reconstructs ledger
   bytes and resolves provisional execution results in Pack-index order. */
static void
test_bam_pair_run( int variant,
                    int reverse,
                    test_bam_pair_result_t * result ) {
  fd_memset( result, 0, sizeof(*result) );
  test_env_t * env[2] = { test_env_create(), NULL };
  env[1] = test_env_create_worker( env[0] );
  fd_bank_t * bank = fd_svm_mini_bank( mini, env[0]->bank_idx );
  int duplicate = variant>=8;
  int dependency = variant==7;
  int instruction_failure = variant==3 || variant==4 || variant==5;
  int fees_only = variant==6;
  int atomic_failure = variant==3 || variant==4;
  ulong next = (variant==2 || variant==4 || duplicate) ? 2UL : 1UL;
  ulong second_worker = reverse && !duplicate ? 1UL : 0UL;
  result->atomic = (variant>=1 && variant<=4) || duplicate;
  result->slot = result->target_slot = bank->f.slot;
  result->first_batch_cnt = next;
  result->second_batch_cnt = duplicate ? 2UL : 1UL;
  result->forwarded_cnt = next+result->second_batch_cnt;
  ulong const fee = 5000UL;
  ((fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue ))->lamports_per_signature = fee;
  uchar private_keys[5][32];
  fd_pubkey_t accounts[5];
  ulong initial[5] = { 1000000000UL, 1000000UL, 1000000UL, 1000000000UL, 1000000UL };
  for( ulong i=0UL; i<5UL; i++ ) {
    fd_memset( private_keys[i], (int)(i+1UL), 32UL );
    fd_sha512_t sha[1];
    FD_TEST( fd_ed25519_public_from_private( accounts[i].uc, private_keys[i], fd_sha512_join( fd_sha512_new( sha ) ) ) );
    fd_sha512_delete( fd_sha512_leave( sha ) );
    test_fund_account( env[0], &accounts[i], initial[i] );
  }
  for( ulong i=0UL; i<next; i++ ) {
    ulong transfer = dependency ? 1000000UL : 1000UL*(i+1UL);
    if( instruction_failure && i==next-1UL ) transfer = ULONG_MAX;
    if( fees_only ) test_build_missing_program_txn( &result->forwarded[i], bank, accounts[0], (fd_pubkey_t){ .ul={0xDEADUL} } );
    else test_build_system_transfer_txns( &result->forwarded[i], bank, accounts[0], &accounts[i+1UL], &transfer, 1UL );
    test_bam_pair_sign( &result->forwarded[i], &accounts[0], private_keys[0] );
  }
  test_mark_bam_batch( result->forwarded, next, 100U+(uint)(2*variant), result->atomic );
  if( duplicate ) {
    fd_memcpy( &result->forwarded[next], result->forwarded, next*sizeof(fd_txn_p_t) );
    if( variant==9 ) {
      ulong transfer = 3000UL;
      test_build_system_transfer_txns( &result->forwarded[next+1UL], bank, accounts[3], &accounts[4], &transfer, 1UL );
      test_bam_pair_sign( &result->forwarded[next+1UL], &accounts[3], private_keys[3] );
    }
  } else {
    ulong payer = dependency ? 1UL : 3UL;
    ulong transfer = dependency ? 800000UL : 3000UL;
    test_build_system_transfer_txns( &result->forwarded[next], bank, accounts[payer], &accounts[4], &transfer, 1UL );
    test_bam_pair_sign( &result->forwarded[next], &accounts[payer], private_keys[payer] );
  }
  test_mark_bam_batch( &result->forwarded[next], result->second_batch_cnt, 101U+(uint)(2*variant), duplicate );
  for( ulong i=0UL; i<result->forwarded_cnt; i++ ) result->forwarded[i].bam.scheduler_gen = 7U;

  fd_pack_limits_t limits = { .max_cost_per_block=48000000UL, .max_vote_cost_per_block=36000000UL,
      .max_write_cost_per_acct=12000000UL, .max_data_bytes_per_block=5UL<<20,
      .max_txn_per_microblock=8UL, .max_microblocks_per_block=32UL,
      .max_allocated_data_per_block=FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK };
  void * mem = fd_wksp_alloc_laddr( mini->wksp, fd_pack_align(), fd_pack_footprint( 64UL, 48UL, 2UL, &limits ), TOPO_TAG );
  FD_TEST( mem );
  fd_rng_t rng[1];
  FD_TEST( fd_rng_join( fd_rng_new( rng, 0U, 0UL ) ) );
  fd_pack_t * pack = fd_pack_join( fd_pack_new( mem, 64UL, 48UL, 2UL, &limits, NULL, 0UL, rng ) );
  FD_TEST( pack );
  fd_pack_set_initializer_bundles_ready( pack );
  test_bam_pair_insert( pack, result->forwarded, next, bank->f.slot );
  if( !duplicate ) test_bam_pair_insert( pack, &result->forwarded[next], result->second_batch_cnt, bank->f.slot );
  fd_txn_e_t dispatch[2][FD_PACK_MAX_TXN_PER_BUNDLE];
  ulong hint;
  FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint ) );
  FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, 48000000UL, 0.0f, 0UL,
               FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BAM_READY,
               hint, dispatch[0] )==next );
  if( duplicate ) test_bam_pair_insert( pack, &result->forwarded[next], result->second_batch_cnt, bank->f.slot );
  test_bam_worker_output_t output[2];
  test_bam_worker_output_init( &output[0], 0UL );
  test_bam_worker_output_init( &output[1], 1UL );
  uint start_idx = UINT_MAX-1U; /* every three-member fixture crosses Pack-index wrap */
  test_bam_poh_fixture_t * poh = test_bam_poh_fixture_new( mini->wksp, bank->f.slot, start_idx );
  if( reverse && dependency ) {
    FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint ) );
    FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, 48000000UL, 0.0f, 1UL,
                 FD_PACK_SCHEDULE_BAM_SINGLE | FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BAM_READY,
                 hint, dispatch[1] )==0UL );
    FD_TEST( fd_pack_avail_txn_cnt( pack )==1UL );
  }
  if( !reverse || dependency || duplicate ) {
    test_bam_execute_pack_output( env[0], &output[0], dispatch[0], next, start_idx, 0UL );
    FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
    if( !reverse )
      for( ulong i=0UL; i<output[0].seqs[0]; i++ ) FD_TEST( !test_bam_poll_poh( poh, env[0], &output[0], i ) );
  }
  FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint ) );
  FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, 48000000UL, 0.0f, second_worker,
               (duplicate ? FD_PACK_SCHEDULE_BUNDLE : FD_PACK_SCHEDULE_BAM_SINGLE) | FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BAM_READY,
               hint, dispatch[1] )==result->second_batch_cnt );
  FD_TEST( !fd_pack_avail_txn_cnt( pack ) );
  ulong first_poh_cnt = output[0].seqs[0];
  ulong second_poh_begin = output[second_worker].seqs[0];
  test_bam_execute_pack_output( env[second_worker], &output[second_worker], dispatch[1], result->second_batch_cnt, start_idx+(uint)next, next );
  FD_TEST( fd_pack_microblock_complete( pack, second_worker )==1 );
  if( reverse ) {
    FD_TEST( test_bam_poll_poh( poh, env[second_worker], &output[second_worker], second_poh_begin ) );
    FD_TEST( !test_bam_poh_fixture_summary( poh )->txn_cnt );
    FD_TEST( !test_bam_poh_fixture_summary( poh )->result_cnt );
    FD_TEST( test_bam_poh_fixture_summary( poh )->expect_pack_idx==start_idx );
    if( !dependency && !duplicate ) {
      test_bam_execute_pack_output( env[0], &output[0], dispatch[0], next, start_idx, 0UL );
      FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
      first_poh_cnt = output[0].seqs[0];
    }
    for( ulong i=0UL; i<first_poh_cnt; i++ ) FD_TEST( !test_bam_poll_poh( poh, env[0], &output[0], i ) );
  }
  for( ulong i=second_poh_begin; i<output[second_worker].seqs[0]; i++ )
    FD_TEST( !test_bam_poll_poh( poh, env[second_worker], &output[second_worker], i ) );
  result->poh = *test_bam_poh_fixture_summary( poh );
  FD_TEST( result->poh.expect_pack_idx==start_idx+(uint)result->forwarded_cnt );
  FD_TEST( result->poh.txn_cnt==(duplicate ? next : (atomic_failure ? 1UL : result->forwarded_cnt)) );
  for( ulong i=0UL; i<result->poh.txn_cnt; i++ ) {
    fd_txn_p_t const * expected = &result->forwarded[atomic_failure ? next : i];
    FD_TEST( result->poh.txns[i].payload_sz==expected->payload_sz );
    FD_TEST( !memcmp( result->poh.txns[i].payload, expected->payload, expected->payload_sz ) );
  }
  ulong terminals = 0UL;
  ulong terminal_seen = 0UL;
  for( ulong i=0UL; i<result->poh.result_cnt; i++ ) {
    fd_bam_bundle_result_t const * r = &result->poh.results[i];
    ulong batch = (ulong)(r->seq_id-(100U+(uint)(2*variant)));
    FD_TEST( batch<2UL && r->scheduler_gen==7U && r->slot==bank->f.slot );
    FD_TEST( !(terminal_seen & (1UL<<batch)) );
    terminal_seen |= 1UL<<batch;
    result->terminal[batch] = *r;
    terminals++;
  }
  for( ulong worker=0UL; worker<2UL; worker++ ) {
    for( ulong i=0UL; i<output[worker].seqs[2]; i++ ) {
      fd_frag_meta_t const * m = output[worker].mcaches[2]+fd_mcache_line_idx( i, output[worker].depths[2] );
      fd_bam_bundle_result_t const * r = fd_chunk_to_laddr_const( env[worker]->execle->out_bam->mem, m->chunk );
      ulong batch = (ulong)(r->seq_id-(100U+(uint)(2*variant)));
      FD_TEST( m->sz==sizeof(*r) && batch<2UL && r->scheduler_gen==7U && r->slot==bank->f.slot );
      FD_TEST( !(terminal_seen & (1UL<<batch)) );
      terminal_seen |= 1UL<<batch;
      result->terminal[batch] = *r;
      terminals++;
    }
  }
  FD_TEST( terminals==2UL && terminal_seen==3UL );
  FD_TEST( result->terminal[0].execution_success==!atomic_failure );
  FD_TEST( result->terminal[1].execution_success==!duplicate );
  if( duplicate ) {
    FD_TEST( result->terminal[1].transaction_err_count==2U );
    FD_TEST( result->terminal[1].transaction_err[0]==bam_types_TransactionErrorReason_ALREADY_PROCESSED );
    FD_TEST( result->terminal[1].transaction_err[1]==bam_types_TransactionErrorReason_COMMIT_CANCELLED );
  } else FD_TEST( !result->terminal[1].transaction_err_count );
  if( !instruction_failure && !fees_only )
    FD_TEST( !result->terminal[0].transaction_err_count );
  if( instruction_failure )
    FD_TEST( result->terminal[0].transaction_err[next-1UL]==bam_types_TransactionErrorReason_INSTRUCTION_ERROR );
  if( fees_only )
    FD_TEST( result->terminal[0].transaction_err[0]==bam_types_TransactionErrorReason_PROGRAM_ACCOUNT_NOT_FOUND );

  ulong expected[5];
  fd_memcpy( expected, initial, sizeof(expected) );
  if( !atomic_failure ) {
    expected[0] -= fee*next;
    if( !instruction_failure && !fees_only ) {
      for( ulong i=0UL; i<next; i++ ) {
        ulong transfer = dependency ? 1000000UL : 1000UL*(i+1UL);
        expected[0] -= transfer;
        expected[i+1UL] += transfer;
      }
    }
  }
  if( !duplicate ) {
    expected[dependency ? 1UL : 3UL] -= fee+(dependency ? 800000UL : 3000UL);
    expected[4] += dependency ? 800000UL : 3000UL;
    FD_TEST( result->terminal[1].feepayer_balance_lamports[0]==expected[dependency ? 1UL : 3UL] );
  }
  for( ulong i=0UL; i<5UL; i++ ) {
    result->balances[i] = test_read_lamports( env[0], &accounts[i] );
    if( result->balances[i]!=expected[i] )
      FD_LOG_WARNING(( "BAM fixture balance mismatch variant=%i reverse=%i account=%lu actual=%lu expected=%lu",
                       variant, reverse, i, result->balances[i], expected[i] ));
    FD_TEST( result->balances[i]==expected[i] );
  }
  fd_pack_delete( fd_pack_leave( pack ) );
  fd_rng_delete( fd_rng_leave( rng ) );
  test_env_destroy( env[0] ); /* frees both contexts and all fixture outputs */
}

static void
test_bam_write_hex( FILE * file,
                     void const * bytes,
                     ulong        size ) {
  uchar const * p = bytes;
  fputc( '"', file );
  for( ulong i=0UL; i<size; i++ ) fprintf( file, "%02x", p[i] );
  fputc( '"', file );
}

FD_UNIT_TEST( execle_bam_two_workers_pack_poh ) {
  static char const * names[10] = { "nonrevert_singles", "atomic_singles", "atomic_multi_then_single",
                                  "atomic_single_failure", "atomic_multi_failure", "nonrevert_instruction_error",
                                  "nonrevert_fees_only", "dependent_writer_lock", "duplicate_atomic_retry", "shared_member_atomic_retry" };
  FILE * file = NULL;
  if( test_bam_fixture_path ) {
    file = fopen( test_bam_fixture_path, "w" );
    FD_TEST( file );
    fprintf( file, "{\"schema\":1,\"evidence\":\"local_real_pack_execle_poh_without_validator_root_confirmation\",\"cases\":[" );
  }
  for( int variant=0; variant<10; variant++ ) {
    test_bam_pair_result_t parallel, serial;
    test_bam_pair_run( variant, 1, &parallel );
    test_bam_pair_run( variant, 0, &serial );
    FD_TEST( !memcmp( parallel.balances, serial.balances, sizeof(parallel.balances) ) );
    FD_TEST( parallel.poh.txn_cnt==serial.poh.txn_cnt );
    for( ulong i=0UL; i<parallel.poh.txn_cnt; i++ ) {
      FD_TEST( parallel.poh.txns[i].payload_sz==serial.poh.txns[i].payload_sz );
      FD_TEST( !memcmp( parallel.poh.txns[i].payload, serial.poh.txns[i].payload, parallel.poh.txns[i].payload_sz ) );
    }
    for( ulong i=0UL; i<2UL; i++ ) {
      FD_TEST( parallel.terminal[i].execution_success==serial.terminal[i].execution_success );
      FD_TEST( !memcmp( parallel.terminal[i].transaction_err, serial.terminal[i].transaction_err,
                        sizeof(parallel.terminal[i].transaction_err) ) );
      FD_TEST( !memcmp( parallel.terminal[i].feepayer_balance_lamports, serial.terminal[i].feepayer_balance_lamports,
                        sizeof(parallel.terminal[i].feepayer_balance_lamports) ) );
    }
    if( file ) {
      fprintf( file, "%s{\"name\":\"%s\",\"slot\":%lu,\"dispatch_bank_slot\":%lu,\"poh_acceptance_slot\":%lu,\"scheduler_generation\":7,\"first_batch_count\":%lu,\"second_batch_count\":%lu,\"first_batch_atomic\":%s,\"forwarded\":[",
               variant ? "," : "", names[variant], parallel.target_slot, parallel.slot, parallel.poh.slot, parallel.first_batch_cnt, parallel.second_batch_cnt, parallel.atomic ? "true" : "false" );
      for( ulong i=0UL; i<parallel.forwarded_cnt; i++ ) {
        if( i ) fputc( ',', file );
        fprintf( file, "{\"sequence\":%u,\"member\":%u,\"target_slot\":%lu,\"transaction\":",
                 parallel.forwarded[i].bam.seq_id, (uint)parallel.forwarded[i].bam.batch_idx, parallel.target_slot );
        test_bam_write_hex( file, parallel.forwarded[i].payload, parallel.forwarded[i].payload_sz );
        fputc( '}', file );
      }
      fprintf( file, "],\"poh_accepted_transactions\":[" );
      for( ulong i=0UL; i<parallel.poh.txn_cnt; i++ ) {
        if( i ) fputc( ',', file );
        test_bam_write_hex( file, parallel.poh.txns[i].payload, parallel.poh.txns[i].payload_sz );
      }
      fprintf( file, "],\"terminal\":[" );
      for( ulong i=0UL; i<2UL; i++ ) {
        fd_bam_bundle_result_t const * r = &parallel.terminal[i];
        fprintf( file, "%s{\"sequence\":%u,\"slot\":%lu,\"execution_success\":%s,\"transaction_errors\":[",
                 i ? "," : "", r->seq_id, r->slot, r->execution_success ? "true" : "false" );
        for( ulong j=0UL; j<(r->transaction_err_count ? r->bundle_txn_cnt : 0UL); j++ )
          fprintf( file, "%s%u", j ? "," : "", (uint)r->transaction_err[j] );
        fprintf( file, "]}" );
      }
      fprintf( file, "],\"balances\":[%lu,%lu,%lu,%lu,%lu]}", parallel.balances[0], parallel.balances[1],
               parallel.balances[2], parallel.balances[3], parallel.balances[4] );
    }
    FD_LOG_NOTICE(( "BAM two-worker real-Pack/execle/PoH fixture passed: %s", names[variant] ));
  }
  if( file ) { fprintf( file, "]}\n" ); FD_TEST( !fclose( file ) ); }
}


int
main( int     argc,
      char ** argv ) {
  fd_svm_mini_limits_t limits[1];
  fd_svm_mini_limits_default( limits );
  limits->max_live_slots          = MAX_LIVE_SLOTS;
  limits->max_txn_per_slot        = MAX_TXN_PER_SLOT;
  limits->max_txn_write_locks     = MAX_TX_ACCOUNT_LOCKS;
  limits->wksp_addl_sz            = 5UL<<30;
  limits->accdb_joiner_cnt        = 3UL; /* mini runtime plus two independent execle joins */

  mini = fd_svm_test_boot( &argc, &argv, limits );
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );

  test_bam_fixture_path = fd_env_strip_cmdline_cstr( &argc, &argv, "--bam-fixture-output", NULL, NULL );

  fd_unit_tests( argc, argv );

  FD_LOG_NOTICE(( "pass" ));
  fd_svm_test_halt( mini );
  return 0;
}
