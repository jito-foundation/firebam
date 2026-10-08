/* FireBAM execle tile tests.  test_execle_tile.c is included verbatim so
   these tests share its fixtures.  Helpers whose FireBAM versions also
   drive the bank_bam out link are renamed below and redefined after the
   include; upstream's cases are compiled but not registered here. */

#pragma push_macro("main")
#undef  main
#define main                           test_execle_tile_upstream_main
#define test_env_create                test_execle_upstream_env_create
#define test_stem                      test_execle_upstream_stem
#define test_execle_flush_rebate       test_execle_upstream_flush_rebate
#define test_execle_run                test_execle_upstream_run
#define test_out_poh_trailer_nonbundle test_execle_upstream_out_poh_trailer_nonbundle
#define test_out_poh_trailer_bundle    test_execle_upstream_out_poh_trailer_bundle
#define test_assert_nonbundle_timing   test_execle_upstream_assert_nonbundle_timing
int test_execle_tile_upstream_main( int argc, char ** argv );
#include "../../discof/execle/test_execle_tile.c"
#undef test_assert_nonbundle_timing
#undef test_out_poh_trailer_bundle
#undef test_out_poh_trailer_nonbundle
#undef test_execle_run
#undef test_execle_flush_rebate
#undef test_stem
#undef test_env_create
#undef main
#pragma pop_macro("main")

#include "../pack/fd_pack.h"
#include "test_bam_poh_fixture.h"

/* Register FireBAM cases on their own list so main runs only them. */

static fd_unit_test_t * test_bam_unit_test_head = NULL;
static fd_unit_test_t * test_bam_unit_test_tail = NULL;

static inline void
register_bam_unit_test( fd_unit_test_t * test ) {
  if( test_bam_unit_test_tail ) test_bam_unit_test_tail->next = test;
  else                          test_bam_unit_test_head = test;
  test_bam_unit_test_tail = test;
}

#undef FD_UNIT_TEST
#define FD_UNIT_TEST( name )                                           \
  static void name( void );                                            \
  static fd_unit_test_t name##_test = { #name, name, NULL };           \
  __attribute__((constructor)) static void register_##name( void ) { register_bam_unit_test( &name##_test ); } \
  static void name( void )

static fd_topo_link_t *
test_topo_link_kind( char const * name,
                     ulong        kind_id ) {
  for( ulong i=0UL; i<topo->link_cnt; i++ ) {
    if( !strcmp( topo->links[i].name, name ) && topo->links[i].kind_id==kind_id ) return &topo->links[i];
  }
  FD_LOG_ERR(( "missing test topo link %s", name ));
}

static test_env_t *
test_env_create_worker( test_env_t const * sibling ) {
  test_env_t * env = fd_wksp_alloc_laddr( mini->wksp, alignof(test_env_t), sizeof(test_env_t), TOPO_TAG );
  FD_TEST( env );
  memset( env, 0, sizeof(test_env_t) );
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) ); /* counters start at zero per test */

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
  fd_topo_tile_t * topo_tile = fd_topob_tile( topo, "execle", "execle", "execle", 0UL, 0, 0, 0, 0 );
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

  /* Back the pack_execle in's fseq the tile joins to return credits */
  void * in_fseq_mem = fd_wksp_alloc_laddr( env->mini->wksp, fd_fseq_align(), fd_fseq_footprint(), TOPO_TAG );
  env->pack_in_fseq = fd_fseq_join( fd_fseq_new( in_fseq_mem, 0UL ) );
  FD_TEST( env->pack_in_fseq );
  test_in_fseq[ 0 ] = env->pack_in_fseq;
  fd_topo_obj_t * in_fseq_obj = &topo->objs[ topo_tile->in_link_fseq_obj_id[ 0UL ] ];
  in_fseq_obj->offset = (ulong)fd_wksp_gaddr_fast( topo->workspaces[ in_fseq_obj->wksp_id ].wksp, in_fseq_mem );

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
    .in_fseq             = test_in_fseq, /* no sleep object: credit return writes the fseq only */
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

static fd_txn_bam_t test_bam[ MAX_TXN_PER_SLOT ]; /* Attached to BAM members, as pack does */

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
    in_txn[i].txnp->first_seen_nanos = 1000L+(long)pack_txn_idx+(long)i;
    if( txns[i].source_tpu==FD_TXN_M_TPU_SOURCE_BAM ) in_txn[i].bam = test_bam[i];
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
  ulong seq = env->in_seq++;
  FD_TEST( !before_frag( env->execle, 0UL, seq, sig ) );
  during_frag( env->execle, 0UL, seq, sig, in_chunk, sz, 0UL );

  fd_stem_context_t stem[1];
  env->begin_tspub = (ulong)fd_frag_meta_ts_comp( fd_tickcount() );
  after_frag( env->execle, 0UL, seq, sig, sz, 0UL, env->begin_tspub, test_stem( env->execle, stem ) );
  /* Pack sees the microblock done and has its credit back */
  FD_TEST( fd_fseq_query( env->execle->busy_fseq )==seq );
  FD_TEST( fd_fseq_query( env->pack_in_fseq )==seq+1UL );

  /* Ordinary microblocks retain every member's ingress time; atomic
     bundles split into one output per member without losing its time. */
  fd_topo_link_t const * out = test_topo_link( "execle_poh" );
  for( ulong i=0UL; i<(is_bundle ? txn_cnt : 1UL); i++ ) {
    fd_frag_meta_t const * meta = out->mcache + fd_mcache_line_idx( i, out->depth );
    fd_txn_p_t const * out_txns = fd_chunk_to_laddr_const( env->execle->out_poh->mem, meta->chunk );
    for( ulong j=0UL; j<(is_bundle ? 1UL : txn_cnt); j++ )
      FD_TEST( out_txns[j].first_seen_nanos==1000L+(long)pack_txn_idx+(long)i+(long)j );
  }
}

static void
test_mark_bam_batch( fd_txn_p_t *   txns,
                     fd_txn_bam_t * bam,
                     ulong          txn_cnt,
                     uint           seq_id,
                     int            revert_on_error ) {
  for( ulong i=0UL; i<txn_cnt; i++ ) {
    txns[ i ].source_tpu = FD_TXN_M_TPU_SOURCE_BAM;
    bam [ i ]            = (fd_txn_bam_t){ .seq_id=seq_id, .batch_idx=(uchar)i, .revert_on_error=!!revert_on_error };
  }
}

static fd_microblock_trailer_t const *
test_out_poh_trailer_nonbundle( test_env_t * env,
                                ulong        txn_cnt ) {
  fd_frag_meta_t const * meta = test_out_poh_meta( 0UL );
  uchar const * data = fd_chunk_to_laddr( env->execle->out_poh->mem, meta->chunk );
  (void)txn_cnt;
  return (fd_microblock_trailer_t const *)( data + meta->sz - sizeof(fd_microblock_trailer_t) );
}

static fd_microblock_trailer_t const *
test_out_poh_trailer_bundle( test_env_t * env,
                             ulong        seq ) {
  fd_frag_meta_t const * meta = test_out_poh_meta( seq );
  uchar const * data = fd_chunk_to_laddr( env->execle->out_poh->mem, meta->chunk );
  return (fd_microblock_trailer_t const *)(data+meta->sz-sizeof(fd_microblock_trailer_t));
}

/* tsorig is pack's publish time; exec_start_ticks is the first
   transaction's load_start_ticks when there is one; tspub is the end
   of execution, after exec_start_ticks and strictly after it once a
   transaction ran (the tile keeps no other copy of that tick). */
static void
test_assert_nonbundle_timing( test_env_t * env,
                              ulong        txn_cnt ) {
  fd_frag_meta_t const *          meta    = test_out_poh_meta( 0UL );
  fd_microblock_trailer_t const * trailer = test_out_poh_trailer_nonbundle( env, txn_cnt );
  FD_TEST( meta->tsorig==(uint)env->begin_tspub );
  if( txn_cnt ) FD_TEST( trailer->exec_start_ticks==env->execle->txn_out[0].details.load_start_ticks );
  long end_ticks = fd_frag_meta_ts_decomp( meta->tspub, trailer->exec_start_ticks );
  FD_TEST( txn_cnt ? end_ticks>trailer->exec_start_ticks : end_ticks>=trailer->exec_start_ticks );
}

/* Folding transaction or loaded-data costs into consumed_cus overcharges work
   that did not execute.  Keep 150 execution CUs and 206 loaded bytes separate. */
FD_UNIT_TEST( execle_bam_result_cus ) {
  fd_bam_bundle_result_t res[1];
  static fd_txn_out_t    txn_out[1];
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

FD_UNIT_TEST( execle_bam_simple_ok ) {
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
  test_mark_bam_batch( txn, test_bam, 1UL, 101U, 0 );
  test_execle_run( env, txn, 1UL, 3U, 17UL, 0 );

  test_assert_nonbundle_timing( env, 1UL );
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

  FD_TEST( fd_metrics_tl[ MIDX( COUNTER, EXECLE, TXN_LANDED )+FD_METRICS_ENUM_TRANSACTION_LANDED_V_LANDED_SUCCESS_IDX ]==1UL );
  FD_TEST( fd_metrics_tl[ MIDX( COUNTER, EXECLE, TXN_RESULT )+FD_METRICS_ENUM_TRANSACTION_RESULT_V_SUCCESS_IDX ]==1UL );
  FD_TEST( fd_metrics_tl[ MIDX( COUNTER, EXECLE, TXN_VERSION )+FD_METRICS_ENUM_TXN_VERSION_V_LEGACY_IDX ]==1UL );
  FD_TEST( env->execle->runtime->metrics.instr_cum==1UL );
  FD_TEST( env->execle->runtime->metrics.cpi_cum==0UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_bam_simple_fee_payer_fail ) {
  /* Transaction failed */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );

  fd_pubkey_t missing_fee_payer = { .ul = { 0x3333UL } };
  fd_pubkey_t data_acct         = { .ul = { 0x4444UL } };
  ulong const data_acct_start = 777UL;
  test_fund_account( env, &data_acct, data_acct_start );

  fd_txn_p_t txn[1];
  test_build_empty_txn( txn, bank, missing_fee_payer, data_acct, 12UL, 0 );
  test_mark_bam_batch( txn, test_bam, 1UL, 102U, 0 );
  test_execle_run( env, txn, 1UL, 4U, 18UL, 0 );

  test_assert_nonbundle_out( env, 1UL, 4U );
  test_assert_nonbundle_timing( env, 1UL );
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

  FD_TEST( fd_metrics_tl[ MIDX( COUNTER, EXECLE, TXN_LANDED )+FD_METRICS_ENUM_TRANSACTION_LANDED_V_UNLANDED_IDX ]==1UL );
  FD_TEST( fd_metrics_tl[ MIDX( COUNTER, EXECLE, TXN_RESULT )+FD_METRICS_ENUM_TRANSACTION_RESULT_V_ACCOUNT_NOT_FOUND_IDX ]==1UL );
  FD_TEST( fd_metrics_tl[ MIDX( COUNTER, EXECLE, TXN_VERSION )+FD_METRICS_ENUM_TXN_VERSION_V_V0_IDX ]==1UL );

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
  test_mark_bam_batch( txn, test_bam, 1UL, 104U, 0 );
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

FD_UNIT_TEST( execle_bam_bundle_ok ) {
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
  test_mark_bam_batch( txns, test_bam, 2UL, 103U, 1 );
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

  FD_TEST( fd_metrics_tl[ MIDX( COUNTER, EXECLE, TXN_LANDED  )+FD_METRICS_ENUM_TRANSACTION_LANDED_V_LANDED_SUCCESS_IDX ]==2UL );
  FD_TEST( fd_metrics_tl[ MIDX( COUNTER, EXECLE, TXN_RESULT  )+FD_METRICS_ENUM_TRANSACTION_RESULT_V_SUCCESS_IDX ]==2UL );
  FD_TEST( fd_metrics_tl[ MIDX( COUNTER, EXECLE, TXN_VERSION )+FD_METRICS_ENUM_TXN_VERSION_V_LEGACY_IDX ]==2UL );

  test_env_destroy( env );
}

FD_UNIT_TEST( execle_bundle_prepare_fail ) {
  /* Preparation can fail before any member executes.  Exercise both
     preparation error exits and reuse outputs from an earlier bundle. */
  for( ulong variant=0UL; variant<4UL; variant++ ) {
    test_env_t * env = test_env_create();
    fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
    fd_pubkey_t payer = { .ul = { 0xabc0UL } };
    fd_pubkey_t recipient = { .ul = { 0xabc1UL } };
    fd_pubkey_t missing_alt = { .ul = { 0xabc2UL } };
    ulong const payer_start = 1000000000UL;
    ulong const recipient_start = 1000000UL;
    ulong const fee = 5000UL;
    fd_blockhash_info_t * blockhash_info = (fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue );
    FD_TEST( blockhash_info );
    blockhash_info->lamports_per_signature = fee;
    test_fund_account( env, &payer, payer_start );
    test_fund_account( env, &recipient, recipient_start );

    fd_txn_p_t txns[3];
    for( ulong i=0UL; i<3UL; i++ )
      test_build_system_transfer_txn( &txns[i], bank, payer, recipient, 11UL+i );
    test_execle_run( env, txns, 3UL, 0U, 0UL, 1 );
    test_execle_flush_rebate( env );
    FD_TEST( test_read_lamports( env, &payer )==payer_start-3UL*fee-36UL );
    FD_TEST( test_read_lamports( env, &recipient )==recipient_start+36UL );

    ulong failed_idx = variant<3UL ? variant : 1UL;
    bam_types_TransactionErrorReason bam_err = variant<3UL ? bam_types_TransactionErrorReason_ADDRESS_LOOKUP_TABLE_NOT_FOUND
                                                         : bam_types_TransactionErrorReason_ACCOUNT_LOADED_TWICE;
    int runtime_err = variant<3UL ? FD_RUNTIME_TXN_ERR_ADDRESS_LOOKUP_TABLE_NOT_FOUND
                                 : FD_RUNTIME_TXN_ERR_ACCOUNT_LOADED_TWICE;
    for( ulong i=0UL; i<3UL; i++ ) {
      test_build_empty_txn( &txns[i], bank, payer, variant==3UL && i==failed_idx ? payer : recipient, 700UL+i, 0 );
      txns[i].flags |= FD_TXN_P_FLAGS_SANITIZE_SUCCESS | FD_TXN_P_FLAGS_EXECUTE_SUCCESS;
      env->execle->txn_out[i].err.txn_err = FD_RUNTIME_TXN_ERR_BLOCKHASH_NOT_FOUND;
    }
    if( variant<3UL ) {
      /* Replace the empty v0 lookup list with a real lookup of a table
         absent from the parent fork.  Keep the transaction parseable. */
      fd_txn_p_t * txn = &txns[failed_idx];
      ulong sz = txn->payload_sz;
      FD_TEST( txn->payload[sz-1UL]==0U );
      txn->payload[sz-1UL] = 1U;
      fd_memcpy( txn->payload+sz, missing_alt.uc, sizeof(fd_pubkey_t) );
      sz += sizeof(fd_pubkey_t);
      txn->payload[sz++] = 1U; /* one writable lookup */
      txn->payload[sz++] = 0U; /* table index */
      txn->payload[sz++] = 0U; /* no readonly lookups */
      txn->payload_sz = (ushort)sz;
      FD_TEST( fd_txn_parse( txn->payload, sz, TXN(txn), NULL ) );
    }
    test_mark_bam_batch( txns, test_bam, 3UL, 106U, 1 );
    test_execle_run( env, txns, 3UL, 3U, 3UL, 1 );
    test_assert_bundle_out( env, 3UL, 3U );

    fd_frag_meta_t const * bam_meta = test_topo_link( "bank_bam" )->mcache;
    FD_TEST( bam_meta->sz==sizeof(fd_bam_bundle_result_t) );
    fd_bam_bundle_result_t const * result = fd_chunk_to_laddr( env->execle->out_bam->mem, bam_meta->chunk );
    FD_TEST( result->seq_id==106U && result->bundle_txn_cnt==3U );
    FD_TEST( !result->execution_success && result->scheduling_error==FD_BAM_SCHED_ERR_NONE );
    FD_TEST( result->transaction_err_count==3U );
    for( ulong i=0UL; i<3UL; i++ ) {
      FD_TEST( result->transaction_err[i]==(i==failed_idx ? bam_err : bam_types_TransactionErrorReason_COMMIT_CANCELLED) );
      FD_TEST( result->sanitize_success[i] );
      FD_TEST( !result->consumed_cus[i] && !result->loaded_accounts_data_size[i] && !result->feepayer_balance_lamports[i] );
      FD_TEST( !env->execle->txn_out[i].err.is_committable );
      FD_TEST( env->execle->txn_out[i].err.txn_err==(i==failed_idx ? runtime_err : FD_RUNTIME_EXECUTE_SUCCESS) );
      fd_frag_meta_t const * meta = test_out_poh_meta( i );
      fd_txn_p_t const * out = fd_chunk_to_laddr( env->execle->out_poh->mem, meta->chunk );
      FD_TEST( !(out->flags & (FD_TXN_P_FLAGS_SANITIZE_SUCCESS | FD_TXN_P_FLAGS_EXECUTE_SUCCESS)) );
      int txn_err = i==failed_idx ? runtime_err : FD_RUNTIME_TXN_ERR_BUNDLE_PEER;
      FD_TEST( (out->flags & FD_TXN_P_FLAGS_RESULT_MASK)==((uint)-txn_err<<24) );
      FD_TEST( !out->execle_cu.actual_consumed_cus );
      FD_TEST( out->execle_cu.rebated_cus==301000U );
      fd_microblock_trailer_t const * trailer = test_out_poh_trailer_bundle( env, i );
      FD_TEST( !trailer->tips );
      FD_TEST( trailer->txn_ns_dt.load_start==0.f && trailer->txn_ns_dt.check_start==0.f );
      FD_TEST( trailer->txn_ns_dt.exec_start==0.f && trailer->txn_ns_dt.commit_start==0.f && trailer->txn_ns_dt.commit_end==0.f );
    }
    FD_TEST( !env->execle->runtime->accounts.account_cnt && !env->execle->runtime->accounts.executable_cnt );
    FD_TEST( test_read_lamports( env, &payer )==payer_start-3UL*fee-36UL );
    FD_TEST( test_read_lamports( env, &recipient )==recipient_start+36UL );
    FD_TEST( fd_metrics_tl[ MIDX( COUNTER, EXECLE, TXN_RESULT )+FD_METRICS_ENUM_TRANSACTION_RESULT_V_BUNDLE_PEER_IDX ]==2UL );
    FD_TEST( fd_metrics_tl[ MIDX( COUNTER, EXECLE, TXN_LANDED )+FD_METRICS_ENUM_TRANSACTION_LANDED_V_UNLANDED_IDX ]==3UL );
    test_execle_flush_rebate( env );
    fd_pack_rebate_t const * rebate = fd_chunk_to_laddr( env->execle->out_pack->mem, test_out_pack_meta( 0UL )->chunk );
    FD_TEST( rebate->total_cost_rebate==903000UL && rebate->microblock_cnt_rebate==3UL );

    /* A subsequent bundle must still acquire, execute, and commit. */
    for( ulong i=0UL; i<3UL; i++ )
      test_build_system_transfer_txn( &txns[i], bank, payer, recipient, 21UL+i );
    test_execle_run( env, txns, 3UL, 6U, 6UL, 1 );
    FD_TEST( test_read_lamports( env, &payer )==payer_start-6UL*fee-102UL );
    FD_TEST( test_read_lamports( env, &recipient )==recipient_start+102UL );
    FD_TEST( !env->execle->runtime->accounts.account_cnt && !env->execle->runtime->accounts.executable_cnt );
    test_env_destroy( env );
  }
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
  ulong * in_fseq[1];
} test_bam_worker_output_t;

static void
test_bam_worker_output_init( test_bam_worker_output_t * out,
                             ulong                      worker,
                             test_env_t *               env ) {
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
  out->in_fseq[0] = env->pack_in_fseq;
  *out->stem = (fd_stem_context_t){ .mcaches=out->mcaches, .seqs=out->seqs, .depths=out->depths,
      .cr_avail=out->credits, .min_cr_avail=&out->min_credit, .cr_decrement_amount=1UL,
      .out_reliable=out->reliable, .in_fseq=out->in_fseq };
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

/* Transfer builders put System last as the only readonly static account.
   Request its write lock in the signed message; runtime must still demote
   it, while Pack must charge the requested lock and allow parallel users. */
static void
test_bam_request_system_write( fd_txn_p_t * txnp ) {
  fd_txn_t * txn = TXN(txnp);
  FD_TEST( txn->readonly_unsigned_cnt==1U );
  fd_acct_addr_t const * keys = fd_txn_get_acct_addrs( txn, txnp->payload );
  FD_TEST( !memcmp( keys+txn->acct_addr_cnt-1U, &fd_solana_system_program_id, 32UL ) );
  ulong header = txn->message_off+(ulong)(txn->transaction_version==FD_TXN_V0);
  txnp->payload[header+2UL] = 0U;
  FD_TEST( fd_txn_parse( txnp->payload, txnp->payload_sz, txn, NULL ) );
  FD_TEST( fd_txn_is_writable( txn, (ushort)(txn->acct_addr_cnt-1U) ) );
}

typedef struct {
  ulong target_slot;
  ulong slot;
  ulong first_batch_cnt;
  ulong second_batch_cnt;
  ulong forwarded_cnt;
  int atomic;
  fd_txn_p_t forwarded[4];
  fd_txn_bam_t bam[4];
  test_bam_poh_summary_t poh;
  fd_bam_bundle_result_t terminal[2];
  ulong balances[5];
} test_bam_pair_result_t;

static void
test_bam_pair_insert( fd_pack_t * pack,
                       fd_txn_p_t const * txns,
                       fd_txn_bam_t const * bam,
                       ulong count,
                       ulong block_height ) {
  fd_txn_e_t * slots[FD_PACK_MAX_TXN_PER_BUNDLE];
  fd_pack_insert_bundle_init( pack, slots, count );
  for( ulong i=0UL; i<count; i++ ) {
    fd_memset( slots[i], 0, sizeof(fd_txn_e_t) );
    *slots[i]->txnp = txns[i];
    slots[i]->bam   = bam[i];
  }
  ulong deleted;
  FD_TEST( fd_pack_insert_bundle_fini( pack, slots, count, block_height, FD_PACK_IB_TYPE_NONE,
                                       NULL, &deleted )>=0 );
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
  int duplicate = variant==8 || variant==9;
  int reserved_write = variant==10 || variant==11;
  int dependency = variant==7;
  int instruction_failure = variant==3 || variant==4 || variant==5;
  int fees_only = variant==6;
  int atomic_failure = variant==3 || variant==4;
  /* The shared-member retry starts with one landed transaction, then
     retries it alongside a new member.  Match the BAM capture consumer. */
  ulong next = (variant==2 || variant==4 || variant==8 || variant==11) ? 2UL : 1UL;
  ulong second_worker = reverse && !duplicate ? 1UL : 0UL;
  result->atomic = (variant>=1 && variant<=4) || duplicate || variant==11;
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
    if( reserved_write ) test_bam_request_system_write( &result->forwarded[i] );
    test_bam_pair_sign( &result->forwarded[i], &accounts[0], private_keys[0] );
  }
  test_mark_bam_batch( result->forwarded, result->bam, next, 100U+(uint)(2*variant), result->atomic );
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
    if( reserved_write ) test_bam_request_system_write( &result->forwarded[next] );
    test_bam_pair_sign( &result->forwarded[next], &accounts[payer], private_keys[payer] );
  }
  test_mark_bam_batch( &result->forwarded[next], &result->bam[next], result->second_batch_cnt, 101U+(uint)(2*variant), duplicate );
  for( ulong i=0UL; i<result->forwarded_cnt; i++ ) result->bam[i].scheduler_gen = 7U;

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
  test_bam_pair_insert( pack, result->forwarded, result->bam, next, bank->f.block_height );
  if( !duplicate ) test_bam_pair_insert( pack, &result->forwarded[next], &result->bam[next], result->second_batch_cnt, bank->f.block_height );
  fd_txn_e_t dispatch[2][FD_PACK_MAX_TXN_PER_BUNDLE];
  ulong hint;
  FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL ) );
  FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, 48000000UL, 0.0f, 0UL,
               FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BAM_READY,
               hint, NULL, NULL, dispatch[0] )==next );
  if( duplicate ) test_bam_pair_insert( pack, &result->forwarded[next], &result->bam[next], result->second_batch_cnt, bank->f.block_height );
  test_bam_worker_output_t output[2];
  test_bam_worker_output_init( &output[0], 0UL, env[0] );
  test_bam_worker_output_init( &output[1], 1UL, env[1] );
  uint start_idx = UINT_MAX-1U; /* every three-member fixture crosses Pack-index wrap */
  test_bam_poh_fixture_t * poh = test_bam_poh_fixture_new( mini->wksp, bank->f.slot, start_idx );
  if( reverse && dependency ) {
    FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL ) );
    FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, 48000000UL, 0.0f, 1UL,
                 FD_PACK_SCHEDULE_BAM_SINGLE | FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BAM_READY,
                 hint, NULL, NULL, dispatch[1] )==0UL );
    FD_TEST( fd_pack_avail_txn_cnt( pack )==1UL );
  }
  if( !reverse || dependency || duplicate ) {
    test_bam_execute_pack_output( env[0], &output[0], dispatch[0], next, start_idx, 0UL );
    FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
    if( !reverse )
      for( ulong i=0UL; i<output[0].seqs[0]; i++ ) FD_TEST( !test_bam_poll_poh( poh, env[0], &output[0], i ) );
  }
  FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL ) );
  FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, 48000000UL, 0.0f, second_worker,
               (duplicate ? FD_PACK_SCHEDULE_BUNDLE : FD_PACK_SCHEDULE_BAM_SINGLE) | FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BAM_READY,
               hint, NULL, NULL, dispatch[1] )==result->second_batch_cnt );
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
  ulong terminal_seen = 0UL;
  for( ulong i=0UL; i<result->poh.result_cnt; i++ ) {
    fd_bam_bundle_result_t const * r = &result->poh.results[i];
    ulong batch = (ulong)(r->seq_id-(100U+(uint)(2*variant)));
    FD_TEST( batch<2UL && r->scheduler_gen==7U && r->slot==bank->f.slot );
    FD_TEST( !(terminal_seen & (1UL<<batch)) );
    terminal_seen |= 1UL<<batch;
    result->terminal[batch] = *r;
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
    }
  }
  FD_TEST( terminal_seen==3UL );
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
  /* Settle actual executor reports after lock completion, including fully
     rebated atomic failures.  Lock release alone does not settle CU costs. */
  ulong actual_cost = 0UL;
  for( ulong worker=0UL; worker<2UL; worker++ ) {
    for( ulong seq=0UL; seq<output[worker].seqs[0]; seq++ ) {
      fd_frag_meta_t const * m = output[worker].mcaches[0]+fd_mcache_line_idx( seq, output[worker].depths[0] );
      fd_bam_microblock_view_t view[1];
      fd_txn_p_t const * txns = fd_chunk_to_laddr_const( env[worker]->execle->out_poh->mem, m->chunk );
      FD_TEST( fd_bam_microblock_parse( txns, m->sz, view ) );
      for( ulong i=0UL; i<view->txn_cnt; i++ )
        if( txns[i].flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS ) actual_cost += txns[i].execle_cu.actual_consumed_cus;
    }
    for( ulong seq=0UL; seq<output[worker].seqs[1]; seq++ ) {
      fd_frag_meta_t const * m = output[worker].mcaches[1]+fd_mcache_line_idx( seq, output[worker].depths[1] );
      FD_TEST( fd_frag_meta_seq_query( m )==seq && m->sig==bank->f.slot );
      fd_pack_rebate_t const * rebate = fd_chunk_to_laddr_const( env[worker]->execle->out_pack->mem, m->chunk );
      fd_pack_rebate_cus( pack, rebate );
    }
  }
  FD_TEST( fd_pack_current_block_cost( pack )==actual_cost );
  FD_TEST( fd_bank_cost_tracker_query( bank )->block_cost==actual_cost );
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
                 parallel.bam[i].seq_id, (uint)parallel.bam[i].batch_idx, parallel.target_slot );
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

FD_UNIT_TEST( execle_bam_reserved_system_pack_poh ) {
  for( int variant=10; variant<=11; variant++ ) {
    test_bam_pair_result_t parallel, serial;
    test_bam_pair_run( variant, 1, &parallel );
    test_bam_pair_run( variant, 0, &serial );
    FD_TEST( !memcmp( parallel.balances, serial.balances, sizeof(parallel.balances) ) );
    FD_TEST( parallel.poh.txn_cnt==serial.poh.txn_cnt );
    for( ulong i=0UL; i<parallel.poh.txn_cnt; i++ ) {
      FD_TEST( parallel.poh.txns[i].payload_sz==serial.poh.txns[i].payload_sz );
      FD_TEST( !memcmp( parallel.poh.txns[i].payload, serial.poh.txns[i].payload, parallel.poh.txns[i].payload_sz ) );
    }
  }
}

typedef struct {
  fd_txn_p_t const * txns;
  fd_txn_bam_t const * bam;
  ulong slot;
} test_bam_lookahead_ready_t;

static int
test_bam_lookahead_ready( void const *       _ctx,
                          fd_txn_e_t const * candidate,
                          ulong              txn_cnt ) {
  test_bam_lookahead_ready_t const * ctx = _ctx;
  FD_TEST( ctx->slot!=ULONG_MAX );
  FD_TEST( candidate->txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( candidate->bam.seq_id>=700U && candidate->bam.seq_id<703U );
  FD_TEST( candidate->bam.scheduler_gen==7U && !candidate->bam.batch_idx && txn_cnt==1UL );
  fd_txn_p_t const * expected = &ctx->txns[candidate->bam.seq_id-700U];
  FD_TEST( candidate->bam.revert_on_error==ctx->bam[candidate->bam.seq_id-700U].revert_on_error );
  FD_TEST( candidate->txnp->payload_sz==expected->payload_sz );
  FD_TEST( !memcmp( candidate->txnp->payload, expected->payload, candidate->txnp->payload_sz ) );
  return 1;
}

typedef struct {
  ulong balances[5];
  ulong actual_cost;
  fd_bam_bundle_result_t terminal[3];
} test_bam_lookahead_result_t;

/* A writes X, B spends from X, and C touches independent Y.  Keep A's pack
   locks outstanding while C executes on worker1.  PoH must still reconstruct
   A,C,B by dispatch index, and the final state must match serial A,B,C. */
static void
test_bam_lookahead_execute( int                          atomic,
                            int                          lookahead,
                            test_bam_lookahead_result_t * result ) {
  fd_memset( result, 0, sizeof(*result) );
  test_env_t * env[2] = { test_env_create(), NULL };
  env[1] = test_env_create_worker( env[0] );
  fd_bank_t * bank = fd_svm_mini_bank( mini, env[0]->bank_idx );
  ulong const fee = 5000UL;
  ((fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue ))->lamports_per_signature = fee;
  uchar private_keys[5][32];
  fd_pubkey_t accounts[5];
  ulong initial[5] = { 1000000000UL, 1000000UL, 1000000UL, 1000000000UL, 1000000UL };
  for( ulong i=0UL; i<5UL; i++ ) {
    fd_memset( private_keys[i], (int)(i+11UL), 32UL );
    fd_sha512_t sha[1];
    FD_TEST( fd_ed25519_public_from_private( accounts[i].uc, private_keys[i], fd_sha512_join( fd_sha512_new( sha ) ) ) );
    fd_sha512_delete( fd_sha512_leave( sha ) );
    test_fund_account( env[0], &accounts[i], initial[i] );
  }
  fd_txn_p_t txns[3];
  fd_txn_bam_t bam[3];
  ulong const payer[3] = { 0UL, 1UL, 3UL };
  ulong const recipient[3] = { 1UL, 2UL, 4UL };
  ulong const transfer[3] = { 1000000UL, 800000UL, 3000UL };
  for( ulong i=0UL; i<3UL; i++ ) {
    test_build_system_transfer_txns( &txns[i], bank, accounts[payer[i]], &accounts[recipient[i]], &transfer[i], 1UL );
    test_bam_pair_sign( &txns[i], &accounts[payer[i]], private_keys[payer[i]] );
    test_mark_bam_batch( &txns[i], &bam[i], 1UL, 700U+(uint)i, atomic );
    bam[i].scheduler_gen = 7U;
  }
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
  for( ulong i=0UL; i<3UL; i++ ) test_bam_pair_insert( pack, &txns[i], &bam[i], 1UL, bank->f.block_height );
  int full_flags = FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BAM_READY;
  int secondary_flags = FD_PACK_SCHEDULE_BAM_SINGLE | FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BAM_READY;
  test_bam_lookahead_ready_t ready = { .txns=txns, .bam=bam, .slot=bank->f.slot };
  test_bam_worker_output_t output[2];
  test_bam_worker_output_init( &output[0], 0UL, env[0] );
  test_bam_worker_output_init( &output[1], 1UL, env[1] );
  uint start_idx = UINT_MAX-1U;
  test_bam_poh_fixture_t * poh = test_bam_poh_fixture_new( mini->wksp, bank->f.slot, start_idx );
  fd_txn_e_t dispatch[3];
  ulong hint;
  FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL )->bam.seq_id==700U );
  FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, 48000000UL, 0.0f, 0UL,
                                                             full_flags, hint, NULL, NULL, &dispatch[0] )==1UL );
  if( lookahead ) {
    FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL )->bam.seq_id==701U );
    FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, 48000000UL, 0.0f, 1UL,
                 secondary_flags, hint, test_bam_lookahead_ready, &ready, &dispatch[2] )==1UL );
    FD_TEST( dispatch[2].bam.seq_id==702U );
    test_bam_execute_pack_output( env[1], &output[1], &dispatch[2], 1UL, start_idx+1U, 1UL );
    FD_TEST( fd_pack_microblock_complete( pack, 1UL )==1 );
    FD_TEST( test_read_lamports( env[0], &accounts[1] )==initial[1] ); /* A has not run. */
    FD_TEST( test_read_lamports( env[0], &accounts[4] )==initial[4]+transfer[2] );
    FD_TEST( test_bam_poll_poh( poh, env[1], &output[1], 0UL ) );
    FD_TEST( !test_bam_poh_fixture_summary( poh )->txn_cnt );
    FD_TEST( !test_bam_poh_fixture_summary( poh )->result_cnt );
    /* Finishing C cannot release A's dependency on B. */
    FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL )->bam.seq_id==701U );
    FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, 48000000UL, 0.0f, 1UL,
                 secondary_flags, hint, test_bam_lookahead_ready, &ready, &dispatch[1] )==0UL );
  }
  test_bam_execute_pack_output( env[0], &output[0], &dispatch[0], 1UL, start_idx, 0UL );
  FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
  FD_TEST( !test_bam_poll_poh( poh, env[0], &output[0], 0UL ) );
  if( lookahead ) FD_TEST( !test_bam_poll_poh( poh, env[1], &output[1], 0UL ) );
  FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL )->bam.seq_id==701U );
  ulong worker = lookahead ? 1UL : 0UL;
  FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, 48000000UL, 0.0f, worker,
                                                             full_flags, hint, NULL, NULL, &dispatch[1] )==1UL );
  test_bam_execute_pack_output( env[worker], &output[worker], &dispatch[1], 1UL,
                                start_idx+(lookahead ? 2U : 1U), lookahead ? 2UL : 1UL );
  FD_TEST( fd_pack_microblock_complete( pack, worker )==1 );
  FD_TEST( !test_bam_poll_poh( poh, env[worker], &output[worker], 1UL ) );
  if( !lookahead ) {
    FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL )->bam.seq_id==702U );
    FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, 48000000UL, 0.0f, 0UL,
                                                               full_flags, hint, NULL, NULL, &dispatch[2] )==1UL );
    test_bam_execute_pack_output( env[0], &output[0], &dispatch[2], 1UL, start_idx+2U, 2UL );
    FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
    FD_TEST( !test_bam_poll_poh( poh, env[0], &output[0], 2UL ) );
  }
  FD_TEST( !fd_pack_avail_txn_cnt( pack ) );
  test_bam_poh_summary_t const * summary = test_bam_poh_fixture_summary( poh );
  FD_TEST( summary->txn_cnt==3UL && summary->result_cnt==3UL && summary->expect_pack_idx==start_idx+3U );
  ulong const dispatch_order[3] = { 0UL, lookahead ? 2UL : 1UL, lookahead ? 1UL : 2UL };
  ulong seen = 0UL;
  for( ulong i=0UL; i<3UL; i++ ) {
    fd_txn_p_t const * expected = &txns[dispatch_order[i]];
    FD_TEST( summary->txns[i].payload_sz==expected->payload_sz );
    FD_TEST( !memcmp( summary->txns[i].payload, expected->payload, expected->payload_sz ) );
    fd_bam_bundle_result_t const * r = &summary->results[i];
    ulong batch = (ulong)(r->seq_id-700U);
    FD_TEST( batch<3UL && !(seen & (1UL<<batch)) );
    FD_TEST( r->execution_success && !r->transaction_err_count && r->scheduler_gen==7U && r->slot==bank->f.slot );
    seen |= 1UL<<batch;
    result->terminal[batch] = *r;
  }
  FD_TEST( seen==7UL );
  ulong expected[5];
  fd_memcpy( expected, initial, sizeof(expected) );
  for( ulong i=0UL; i<3UL; i++ ) { expected[payer[i]]-=fee+transfer[i]; expected[recipient[i]]+=transfer[i]; }
  for( ulong i=0UL; i<5UL; i++ ) FD_TEST( (result->balances[i]=test_read_lamports( env[0], &accounts[i] ))==expected[i] );
  for( ulong w=0UL; w<2UL; w++ ) {
    FD_TEST( !output[w].seqs[2] ); /* All terminal successes come from PoH. */
    for( ulong seq=0UL; seq<output[w].seqs[0]; seq++ ) {
      fd_frag_meta_t const * m = output[w].mcaches[0]+fd_mcache_line_idx( seq, output[w].depths[0] );
      fd_txn_p_t const * txn = fd_chunk_to_laddr_const( env[w]->execle->out_poh->mem, m->chunk );
      FD_TEST( txn->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS );
      result->actual_cost += txn->execle_cu.actual_consumed_cus;
    }
    for( ulong seq=0UL; seq<output[w].seqs[1]; seq++ ) {
      fd_frag_meta_t const * m = output[w].mcaches[1]+fd_mcache_line_idx( seq, output[w].depths[1] );
      FD_TEST( fd_frag_meta_seq_query( m )==seq && m->sig==bank->f.slot );
      fd_pack_rebate_cus( pack, fd_chunk_to_laddr_const( env[w]->execle->out_pack->mem, m->chunk ) );
    }
  }
  FD_TEST( fd_pack_current_block_cost( pack )==result->actual_cost );
  FD_TEST( fd_bank_cost_tracker_query( bank )->block_cost==result->actual_cost );
  fd_pack_delete( fd_pack_leave( pack ) );
  fd_rng_delete( fd_rng_leave( rng ) );
  test_env_destroy( env[0] );
}

FD_UNIT_TEST( execle_bam_independent_lookahead_pack_poh ) {
  for( int atomic=0; atomic<2; atomic++ ) {
    test_bam_lookahead_result_t parallel, serial;
    test_bam_lookahead_execute( atomic, 1, &parallel );
    test_bam_lookahead_execute( atomic, 0, &serial );
    FD_TEST( !memcmp( parallel.balances, serial.balances, sizeof(parallel.balances) ) );
    FD_TEST( parallel.actual_cost==serial.actual_cost );
    for( ulong i=0UL; i<3UL; i++ ) {
      fd_bam_bundle_result_t const * p = &parallel.terminal[i];
      fd_bam_bundle_result_t const * s = &serial.terminal[i];
      FD_TEST( p->seq_id==s->seq_id && p->scheduler_gen==s->scheduler_gen && p->slot==s->slot );
      FD_TEST( p->execution_success==s->execution_success && p->bundle_txn_cnt==s->bundle_txn_cnt );
      FD_TEST( p->transaction_err_count==s->transaction_err_count && p->sanitize_success[0]==s->sanitize_success[0] );
      FD_TEST( p->consumed_cus[0]==s->consumed_cus[0] );
      FD_TEST( p->feepayer_balance_lamports[0]==s->feepayer_balance_lamports[0] );
      FD_TEST( p->loaded_accounts_data_size[0]==s->loaded_accounts_data_size[0] );
    }
  }
}

FD_UNIT_TEST( execle_bank_blockhash_validity ) {
  /* Exercise the actual runtime, after conservative resolver/pack admission.
     The bank is hundreds of slots beyond its parent; produced-hash age,
     fork membership and durable nonce state still decide admission. */
  for( ulong kind=0UL; kind<7UL; kind++ ) {
    test_env_t * env = test_env_create();
    ulong parent_idx = env->bank_idx;
    fd_svm_mini_freeze( mini, parent_idx );
    env->bank_idx = fd_svm_mini_attach_child( mini, parent_idx, 600UL );
    fd_bank_t * bank = fd_svm_mini_bank( mini, env->bank_idx );
    fd_hash_t hash = *fd_blockhashes_peek_last_hash( &bank->f.block_hash_queue );
    ((fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue ))->lamports_per_signature = 5000UL;
    ulong produced = kind<3UL ? 149UL+kind : 0UL;
    for( ulong i=0UL; i<produced; i++ ) {
      fd_hash_t next_hash = { .ul={0xabcdefUL, i+1UL, 0UL, 0UL} };
      fd_blockhashes_push_new( &bank->f.block_hash_queue, &next_hash )->lamports_per_signature = 5000UL;
    }
    if( kind==3UL || kind==4UL ) {
      hash = (fd_hash_t){ .ul={0x123456789UL, 0UL, 0UL, 0UL} };
      if( kind==4UL ) {
        ulong sibling_idx = fd_svm_mini_attach_child( mini, parent_idx, 600UL );
        fd_bank_t * sibling = fd_svm_mini_bank( mini, sibling_idx );
        fd_blockhashes_push_new( &sibling->f.block_hash_queue, &hash );
        FD_TEST( fd_blockhashes_check_age( &sibling->f.block_hash_queue, &hash, 150UL ) );
      }
      FD_TEST( !fd_blockhashes_check_age( &bank->f.block_hash_queue, &hash, 150UL ) );
    }

    uchar private_key[32];
    fd_memset( private_key, 37, sizeof(private_key) );
    fd_pubkey_t payer;
    fd_sha512_t sha[1];
    fd_ed25519_public_from_private( payer.uc, private_key, fd_sha512_join( fd_sha512_new( sha ) ) );
    fd_sha512_delete( fd_sha512_leave( sha ) );
    fd_pubkey_t recipient = { .ul={0x7777UL} };
    test_fund_account( env, &payer, 1000000000UL );
    test_fund_account( env, &recipient, 1000000UL );
    fd_txn_p_t txn[1];
    if( kind>=5UL ) {
      fd_pubkey_t nonce_key = { .ul={0x9999UL} };
      fd_hash_t seed = { .ul={0x5555UL} };
      test_durable_nonce_from_blockhash( &hash, &seed );
      fd_hash_t stored = hash;
      if( kind==6UL ) stored.uc[0] ^= 1U;
      test_put_nonce_account_rooted( env, &nonce_key, &payer, &stored, 5000000UL );
      test_build_durable_nonce_transfer_txn( txn, payer, nonce_key, recipient, &hash, 1000UL, 91UL );
    } else {
      test_build_system_transfer_txn( txn, bank, payer, recipient, 1000UL );
      fd_memcpy( txn->payload+TXN(txn)->recent_blockhash_off, &hash, sizeof(hash) );
    }
    test_bam_pair_sign( txn, &payer, private_key );
    test_mark_bam_batch( txn, test_bam, 1UL, 400U+(uint)kind, 0 );
    test_execle_run( env, txn, 1UL, 0U, 0UL, 0 );

    int accepted = kind<2UL || kind==5UL;
    fd_txn_p_t const * out = fd_chunk_to_laddr_const( env->execle->out_poh->mem, test_out_poh_meta( 0UL )->chunk );
    FD_TEST( !!(out->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS)==accepted );
    int expected_err = accepted ? FD_RUNTIME_EXECUTE_SUCCESS :
                       kind==6UL ? FD_RUNTIME_TXN_ERR_BLOCKHASH_FAIL_WRONG_NONCE : FD_RUNTIME_TXN_ERR_BLOCKHASH_NOT_FOUND;
    FD_TEST( env->execle->txn_out[0].err.txn_err==expected_err );
    FD_TEST( test_read_lamports( env, &recipient )==1000000UL+(accepted ? 1000UL : 0UL) );
    if( !accepted ) FD_TEST( test_read_lamports( env, &payer )==1000000000UL );
    test_env_destroy( env );
  }
}

FD_UNIT_TEST( execle_bam_same_nonce_batch ) {
  /* As in jito-solana, pack admits a BAM revert batch [A(N),X,B(N)] that
     advances one durable nonce twice.  The bank fails B, which BAM reports
     as BLOCKHASH_NOT_FOUND at index 2, and nothing commits. */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( env->mini, env->bank_idx );
  fd_pubkey_t payer     = { .ul = { 0xcdc0UL } };
  fd_pubkey_t nonce_key = { .ul = { 0xcdc1UL } };
  fd_pubkey_t recipient = { .ul = { 0xcdc2UL } };
  ulong const start = 10000000000UL;
  ((fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue ))->lamports_per_signature = 5000UL;
  fd_hash_t stale = { .ul = { 0x4242UL } };
  fd_hash_t durable_nonce;
  test_durable_nonce_from_blockhash( &durable_nonce, &stale );
  test_fund_account( env, &payer,     start );
  test_fund_account( env, &recipient, start );
  test_put_nonce_account_rooted( env, &nonce_key, &payer, &durable_nonce, start );

  fd_txn_p_t txns[3];
  test_build_durable_nonce_transfer_txn( &txns[0], payer, nonce_key, recipient, &durable_nonce, 11UL, 61UL );
  test_build_system_transfer_txn( &txns[1], bank, payer, recipient, 12UL );
  test_build_durable_nonce_transfer_txn( &txns[2], payer, nonce_key, recipient, &durable_nonce, 13UL, 62UL );
  test_mark_bam_batch( txns, test_bam, 3UL, 130U, 1 );

  fd_pack_limits_t limits = { .max_cost_per_block=48000000UL, .max_vote_cost_per_block=36000000UL,
      .max_write_cost_per_acct=12000000UL, .max_data_bytes_per_block=5UL<<20,
      .max_txn_per_microblock=8UL, .max_microblocks_per_block=32UL,
      .max_allocated_data_per_block=FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK };
  void * mem = fd_wksp_alloc_laddr( mini->wksp, fd_pack_align(), fd_pack_footprint( 16UL, 48UL, 1UL, &limits ), TOPO_TAG );
  FD_TEST( mem );
  fd_rng_t rng[1];
  FD_TEST( fd_rng_join( fd_rng_new( rng, 0U, 0UL ) ) );
  fd_pack_t * pack = fd_pack_join( fd_pack_new( mem, 16UL, 48UL, 1UL, &limits, NULL, 0UL, rng ) );
  FD_TEST( pack );
  test_bam_pair_insert( pack, txns, test_bam, 3UL, bank->f.block_height );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==3UL );
  fd_pack_delete( fd_pack_leave( pack ) );
  fd_wksp_free_laddr( mem );
  fd_rng_delete( fd_rng_leave( rng ) );

  test_execle_run( env, txns, 3UL, 30U, 61UL, 1 );
  FD_TEST( env->execle->txn_out[2].err.txn_err==FD_RUNTIME_TXN_ERR_BLOCKHASH_FAIL_WRONG_NONCE );
  fd_frag_meta_t const * bam_meta = test_topo_link( "bank_bam" )->mcache;
  fd_bam_bundle_result_t const * result = fd_chunk_to_laddr( env->execle->out_bam->mem, bam_meta->chunk );
  FD_TEST( result->seq_id==130U && result->bundle_txn_cnt==3U && !result->execution_success );
  FD_TEST( result->transaction_err_count==3U );
  for( ulong i=0UL; i<3UL; i++ )
    FD_TEST( result->transaction_err[i]==(i==2UL ? bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND
                                                 : bam_types_TransactionErrorReason_COMMIT_CANCELLED) );
  FD_TEST( test_read_lamports( env, &payer     )==start );
  FD_TEST( test_read_lamports( env, &recipient )==start );
  test_env_destroy( env );
}

FD_UNIT_TEST( execle_reserved_fee_payer_rejected ) {
  /* The pack admission repair must not make a reserved account a usable
     fee payer.  This directly tests runtime validation; no signature with
     the reserved address is constructed or presumed valid. */
  test_env_t * env = test_env_create();
  fd_bank_t * bank = fd_svm_mini_bank( mini, env->bank_idx );
  fd_pubkey_t recipient = { .ul={0xaaaabbbbUL} };
  fd_txn_p_t txn[1];
  test_build_empty_txn( txn, bank, fd_solana_system_program_id, recipient, 93UL, 0 );
  ulong before = test_read_lamports( env, &fd_solana_system_program_id );
  FD_TEST( before>0UL );
  test_execle_run( env, txn, 1UL, 0U, 0UL, 0 );
  FD_TEST( !env->execle->txn_out[0].err.is_committable );
  FD_TEST( env->execle->txn_out[0].err.txn_err==FD_RUNTIME_TXN_ERR_INVALID_ACCOUNT_FOR_FEE );
  FD_TEST( test_read_lamports( env, &fd_solana_system_program_id )==before );
  test_env_destroy( env );
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
  /* run_unit_tests.sh passes --numa-idx, which fd_unit_tests would take
     as a case-name filter and then run nothing. */
  fd_env_strip_cmdline_ulong( &argc, &argv, "--numa-idx", NULL, 0UL );

  fd_unit_test_head = test_bam_unit_test_head;
  fd_unit_test_tail = test_bam_unit_test_tail;
  fd_unit_tests( argc, argv );

  FD_LOG_NOTICE(( "pass" ));
  fd_svm_test_halt( mini );
  return 0;
}
