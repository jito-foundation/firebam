#include <stdlib.h>

#ifndef TEST_BAM_RESOLVE_CTX_T
#error "TEST_BAM_RESOLVE_CTX_T must be defined"
#endif
#ifndef TEST_BAM_RESOLVE_OUT_CNT
#error "TEST_BAM_RESOLVE_OUT_CNT must be defined"
#endif
#ifndef TEST_BAM_RESOLVE_BAM_OUT_IDX
#error "TEST_BAM_RESOLVE_BAM_OUT_IDX must be defined"
#endif
#ifndef TEST_BAM_RESOLVE_IN_KIND
#error "TEST_BAM_RESOLVE_IN_KIND must be defined"
#endif
#ifndef TEST_BAM_RESOLVE_HAS_REPLAY
#define TEST_BAM_RESOLVE_HAS_REPLAY 0
#endif
#ifndef TEST_BAM_RESOLVE_RUN_UNKNOWN_BLOCKHASH
#define TEST_BAM_RESOLVE_RUN_UNKNOWN_BLOCKHASH 0
#endif

#define TEST_MCACHE_DEPTH 16UL
#define TEST_DCACHE_CHUNKS 64UL

FD_STATIC_ASSERT( TEST_BAM_RESOLVE_BAM_OUT_IDX < TEST_BAM_RESOLVE_OUT_CNT,
                  test_bam_resolve_bam_out_idx );

typedef struct {
  TEST_BAM_RESOLVE_CTX_T * ctx;

  uchar pack_dcache[ TEST_DCACHE_CHUNKS*FD_CHUNK_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
#if TEST_BAM_RESOLVE_HAS_REPLAY
  uchar replay_dcache[ TEST_DCACHE_CHUNKS*FD_CHUNK_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
#endif
  uchar bam_dcache [ TEST_DCACHE_CHUNKS*FD_CHUNK_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));

  fd_frag_meta_t pack_mcache[ TEST_MCACHE_DEPTH ] __attribute__((aligned(alignof(fd_frag_meta_t))));
#if TEST_BAM_RESOLVE_HAS_REPLAY
  fd_frag_meta_t replay_mcache[ TEST_MCACHE_DEPTH ] __attribute__((aligned(alignof(fd_frag_meta_t))));
#endif
  fd_frag_meta_t bam_mcache [ TEST_MCACHE_DEPTH ] __attribute__((aligned(alignof(fd_frag_meta_t))));

  fd_frag_meta_t * mcaches[ TEST_BAM_RESOLVE_OUT_CNT ];
  ulong            seqs[ TEST_BAM_RESOLVE_OUT_CNT ];
  ulong            depths[ TEST_BAM_RESOLVE_OUT_CNT ];
  ulong            cr_avail[ TEST_BAM_RESOLVE_OUT_CNT ];
  ulong            min_cr_avail;
  int              out_reliable[ TEST_BAM_RESOLVE_OUT_CNT ];
  fd_stem_context_t stem[1];

  void * map_mem;
  void * pool_mem;
  void * map_chain_mem;
} test_harness_t;

static uchar metrics_scratch[ FD_METRICS_FOOTPRINT( 0UL ) ] __attribute__((aligned(FD_METRICS_ALIGN)));

static void *
test_alloc( ulong align,
            ulong sz ) {
  void * p = NULL;
  FD_TEST( 0==posix_memalign( &p, align, fd_ulong_align_up( sz, align ) ) );
  fd_memset( p, 0, fd_ulong_align_up( sz, align ) );
  return p;
}

static void
test_harness_delete( test_harness_t * h ) {
  free( h->ctx );
  free( h->map_chain_mem );
  free( h->pool_mem );
  free( h->map_mem );
  fd_memset( h, 0, sizeof(test_harness_t) );
}

static void
test_harness_new( test_harness_t * h ) {
  fd_memset( h, 0, sizeof(test_harness_t) );

  h->ctx           = test_alloc( alignof(TEST_BAM_RESOLVE_CTX_T), sizeof(TEST_BAM_RESOLVE_CTX_T) );
  h->map_mem       = test_alloc( map_align(),       map_footprint()            );
  h->pool_mem      = test_alloc( pool_align(),      pool_footprint( 4UL )      );
  h->map_chain_mem = test_alloc( map_chain_align(), map_chain_footprint( 8UL ) );

  h->ctx->blockhash_map = map_join( map_new( h->map_mem ) );
  h->ctx->pool          = pool_join( pool_new( h->pool_mem, 4UL ) );
  h->ctx->map_chain     = map_chain_join( map_chain_new( h->map_chain_mem, 8UL, 0UL ) );
  FD_TEST( h->ctx->blockhash_map );
  FD_TEST( h->ctx->pool );
  FD_TEST( h->ctx->map_chain );
  FD_TEST( h->ctx->lru_list==lru_list_join( lru_list_new( h->ctx->lru_list ) ) );

  h->ctx->round_robin_cnt = 1UL;
  h->ctx->flush_pool_idx  = ULONG_MAX;
  h->ctx->in[ 0 ].kind    = TEST_BAM_RESOLVE_IN_KIND;

  h->ctx->out_pack->idx    = 0UL;
  h->ctx->out_pack->mem    = (fd_wksp_t *)h->pack_dcache;
  h->ctx->out_pack->chunk0 = 0UL;
  h->ctx->out_pack->wmark  = TEST_DCACHE_CHUNKS-1UL;
  h->ctx->out_pack->chunk  = 0UL;

#if TEST_BAM_RESOLVE_HAS_REPLAY
  h->ctx->out_replay->idx    = 1UL;
  h->ctx->out_replay->mem    = (fd_wksp_t *)h->replay_dcache;
  h->ctx->out_replay->chunk0 = 0UL;
  h->ctx->out_replay->wmark  = TEST_DCACHE_CHUNKS-1UL;
  h->ctx->out_replay->chunk  = 0UL;
#endif

  h->ctx->out_bam->idx    = TEST_BAM_RESOLVE_BAM_OUT_IDX;
  h->ctx->out_bam->mem    = (fd_wksp_t *)h->bam_dcache;
  h->ctx->out_bam->chunk0 = 0UL;
  h->ctx->out_bam->wmark  = TEST_DCACHE_CHUNKS-1UL;
  h->ctx->out_bam->chunk  = 0UL;

  h->mcaches[ 0 ] = h->pack_mcache;
#if TEST_BAM_RESOLVE_HAS_REPLAY
  h->mcaches[ 1 ] = h->replay_mcache;
#endif
  h->mcaches[ TEST_BAM_RESOLVE_BAM_OUT_IDX ] = h->bam_mcache;
  for( ulong i=0UL; i<TEST_BAM_RESOLVE_OUT_CNT; i++ ) {
    h->depths[ i ]       = TEST_MCACHE_DEPTH;
    h->cr_avail[ i ]     = ULONG_MAX;
    h->out_reliable[ i ] = 0;
  }
  h->min_cr_avail = ULONG_MAX;

  *h->stem = (fd_stem_context_t) {
    .mcaches             = h->mcaches,
    .seqs                = h->seqs,
    .depths              = h->depths,
    .cr_avail            = h->cr_avail,
    .min_cr_avail        = &h->min_cr_avail,
    .cr_decrement_amount = 0UL,
    .out_reliable        = h->out_reliable,
  };
}

static fd_txn_m_t *
test_prepare_bam_txn( test_harness_t * h,
                      uchar            hash_seed,
                      _Bool            revert_on_error,
                      uchar            batch_idx,
                      uchar            txn_cnt,
                      int              has_alt ) {
  fd_txn_m_t * txnm = (fd_txn_m_t *)h->pack_dcache;
  fd_memset( txnm, 0, FD_TPU_PARSED_MTU );

  txnm->payload_sz = 32U;
  txnm->txn_t_sz   = (ushort)fd_txn_footprint( 0UL, 0UL );
  txnm->source_tpu = FD_TXN_M_TPU_SOURCE_BAM;

  txnm->block_engine.bundle_id      = fd_ulong_if( revert_on_error, 1000UL, 0UL );
  txnm->block_engine.bundle_txn_cnt = fd_ulong_if( revert_on_error && !batch_idx, txn_cnt, 0UL );

  txnm->bam.max_schedule_slot = 500UL;
  txnm->bam.seq_id            = 123U;
  txnm->bam.txn_cnt           = txn_cnt;
  txnm->bam.batch_idx         = batch_idx;
  txnm->bam.revert_on_error   = revert_on_error;

  uchar * payload = fd_txn_m_payload( txnm );
  for( ulong i=0UL; i<32UL; i++ ) payload[ i ] = (uchar)(hash_seed+i+1U);

  fd_txn_t * txnt = fd_txn_m_txn_t( txnm );
  fd_memset( txnt, 0, fd_txn_footprint( 0UL, 0UL ) );
  txnt->recent_blockhash_off = 0U;
  txnt->addr_table_adtl_cnt  = (uchar)has_alt;

  return txnm;
}

static void
test_insert_blockhash( test_harness_t * h,
                       fd_txn_m_t *     txnm,
                       ulong            slot ) {
  blockhash_map_t * entry = map_insert( h->ctx->blockhash_map, *(blockhash_t *)fd_txn_m_payload( txnm ) );
  entry->slot = slot;
}

static fd_bam_bundle_result_t const *
test_last_bam_result( test_harness_t const * h ) {
  FD_TEST( h->seqs[ TEST_BAM_RESOLVE_BAM_OUT_IDX ]>0UL );
  return (fd_bam_bundle_result_t const *)h->bam_dcache;
}

static void
test_expired_non_revert_bam_result( void ) {
  test_harness_t h[1];
  test_harness_new( h );

  fd_txn_m_t * txnm = test_prepare_bam_txn( h, 7U, 0, 0U, 1U, 0 );
  test_insert_blockhash( h, txnm, 10UL );
  h->ctx->completed_slot = 200UL;

  after_frag( h->ctx, 0UL, 0UL, 0UL, fd_txn_m_realized_footprint( txnm, 1, 0 ), 0UL, 0UL, h->stem );

  FD_TEST( h->seqs[ 0 ]==0UL );
  FD_TEST( h->seqs[ TEST_BAM_RESOLVE_BAM_OUT_IDX ]==1UL );
  fd_bam_bundle_result_t const * res = test_last_bam_result( h );
  FD_TEST( res->seq_id                == 123U );
  FD_TEST( res->slot                  == 500UL );
  FD_TEST( res->bundle_txn_cnt        == 1U );
  FD_TEST( res->bundle_err            == FD_BAM_BUNDLE_ERR_NONE );
  FD_TEST( res->transaction_err_count == 1U );
  FD_TEST( res->transaction_err[ 0 ]  == bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );
  FD_TEST( res->sanitize_success[ 0 ] );

  test_harness_delete( h );
}

static void
test_no_bank_deser_result( uchar batch_idx,
                           ulong expected_bam_publishes ) {
  test_harness_t h[1];
  test_harness_new( h );

  fd_txn_m_t * txnm = test_prepare_bam_txn( h, 11U, 1, batch_idx, 2U, 1 );
  test_insert_blockhash( h, txnm, 100UL );
  h->ctx->completed_slot = 100UL;

  after_frag( h->ctx, 0UL, 0UL, 0UL, fd_txn_m_realized_footprint( txnm, 1, 0 ), 0UL, 0UL, h->stem );

  FD_TEST( h->seqs[ 0 ]==0UL );
  FD_TEST( h->seqs[ TEST_BAM_RESOLVE_BAM_OUT_IDX ]==expected_bam_publishes );
  if( FD_LIKELY( expected_bam_publishes ) ) {
    fd_bam_bundle_result_t const * res = test_last_bam_result( h );
    FD_TEST( res->seq_id         == 123U );
    FD_TEST( res->slot           == 500UL );
    FD_TEST( res->bundle_txn_cnt == 2U );
    FD_TEST( res->bundle_err     == FD_BAM_BUNDLE_ERR_DESER );
    FD_TEST( res->deser_index    == batch_idx );
    FD_TEST( res->deser_reason   == bam_types_DeserializationErrorReason_SANITIZE_ERROR );
  }

  test_harness_delete( h );
}

#if TEST_BAM_RESOLVE_RUN_UNKNOWN_BLOCKHASH
static void
test_unknown_blockhash_bam_bypasses_stash( void ) {
  test_harness_t h[1];
  test_harness_new( h );

  fd_txn_m_t * txnm = test_prepare_bam_txn( h, 17U, 0, 0U, 1U, 0 );
  h->ctx->completed_slot = 100UL;

  after_frag( h->ctx, 0UL, 0UL, 0UL, fd_txn_m_realized_footprint( txnm, 1, 0 ), 0UL, 0UL, h->stem );

  FD_TEST( h->seqs[ 0 ]==1UL );
  FD_TEST( h->seqs[ TEST_BAM_RESOLVE_BAM_OUT_IDX ]==0UL );
  FD_TEST( h->ctx->metrics.stash[ FD_METRICS_ENUM_RESOLVE_STASH_OPERATION_V_INSERTED_IDX ]==0UL );

  test_harness_delete( h );
}
#endif

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  fd_metrics_register( fd_metrics_join( fd_metrics_new( metrics_scratch, 0UL ) ) );

  test_expired_non_revert_bam_result();
  test_no_bank_deser_result( 0U, 1UL );
  test_no_bank_deser_result( 1U, 0UL );
#if TEST_BAM_RESOLVE_RUN_UNKNOWN_BLOCKHASH
  test_unknown_blockhash_bam_bypasses_stash();
#endif

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
