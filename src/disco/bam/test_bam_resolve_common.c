#include <stdlib.h>

#ifndef TEST_BAM_RESOLVE_CTX_T
#error "TEST_BAM_RESOLVE_CTX_T must be defined"
#endif
#ifndef TEST_BAM_RESOLVE_OUT_CNT
#error "TEST_BAM_RESOLVE_OUT_CNT must be defined"
#endif
#ifndef TEST_BAM_RESOLVE_IN_KIND
#error "TEST_BAM_RESOLVE_IN_KIND must be defined"
#endif
/* TEST_BAM_RESOLVE_OUT( ctx, f ) names the tile's pack output field f
   (mem, chunk0, wmark or chunk). */
#ifndef TEST_BAM_RESOLVE_OUT
#error "TEST_BAM_RESOLVE_OUT must be defined"
#endif
#ifndef TEST_BAM_RESOLVE_HAS_REPLAY
#define TEST_BAM_RESOLVE_HAS_REPLAY 0
#endif

#define TEST_MCACHE_DEPTH  16UL
#define TEST_DCACHE_CHUNKS 128UL

/* Transaction V1 makes FD_TPU_PARSED_MTU larger than the old 64-chunk test
   dcache.  Keep at least one complete parsed transaction in the ring. */
FD_STATIC_ASSERT( TEST_DCACHE_CHUNKS*FD_CHUNK_SZ>=FD_TPU_PARSED_MTU, test_dcache_too_small );

typedef struct {
  TEST_BAM_RESOLVE_CTX_T * ctx;

  uchar pack_dcache[ TEST_DCACHE_CHUNKS*FD_CHUNK_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
#if TEST_BAM_RESOLVE_HAS_REPLAY
  uchar replay_dcache[ TEST_DCACHE_CHUNKS*FD_CHUNK_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
#endif
  fd_frag_meta_t pack_mcache[ TEST_MCACHE_DEPTH ] __attribute__((aligned(alignof(fd_frag_meta_t))));
#if TEST_BAM_RESOLVE_HAS_REPLAY
  fd_frag_meta_t replay_mcache[ TEST_MCACHE_DEPTH ] __attribute__((aligned(alignof(fd_frag_meta_t))));
#endif

  fd_frag_meta_t * mcaches[ TEST_BAM_RESOLVE_OUT_CNT ];
  ulong            seqs[ TEST_BAM_RESOLVE_OUT_CNT ];
  ulong            depths[ TEST_BAM_RESOLVE_OUT_CNT ];
  ulong            cr_avail[ TEST_BAM_RESOLVE_OUT_CNT ];
  ulong            min_cr_avail;
  int              out_reliable[ TEST_BAM_RESOLVE_OUT_CNT ];
  fd_stem_context_t stem[1];

  void * map_mem;
  void * nonce_map_mem;
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
  free( h->nonce_map_mem );
  fd_memset( h, 0, sizeof(test_harness_t) );
}

static void
test_harness_new( test_harness_t * h ) {
  fd_memset( h, 0, sizeof(test_harness_t) );

  h->ctx           = test_alloc( alignof(TEST_BAM_RESOLVE_CTX_T), sizeof(TEST_BAM_RESOLVE_CTX_T) );
  h->map_mem       = test_alloc( map_align(),       map_footprint( 32UL ) );
  h->nonce_map_mem = test_alloc( map_align(),       map_footprint( 32UL ) );
  h->pool_mem      = test_alloc( pool_align(),      pool_footprint( 4UL )      );
  h->map_chain_mem = test_alloc( map_chain_align(), map_chain_footprint( 8UL ) );

  h->ctx->blockhash_map = map_join( map_new( h->map_mem, 32UL, 0UL ) );
  h->ctx->nonce_blockhash_map = map_join( map_new( h->nonce_map_mem, 32UL, 0UL ) );
  h->ctx->pool          = pool_join( pool_new( h->pool_mem, 4UL ) );
  h->ctx->map_chain     = map_chain_join( map_chain_new( h->map_chain_mem, 8UL, 0UL ) );
  FD_TEST( h->ctx->blockhash_map );
  FD_TEST( h->ctx->nonce_blockhash_map );
  FD_TEST( h->ctx->pool );
  FD_TEST( h->ctx->map_chain );
  FD_TEST( h->ctx->lru_list==lru_list_join( lru_list_new( h->ctx->lru_list ) ) );

  h->ctx->round_robin_cnt = 1UL;
  h->ctx->flush_pool_idx  = ULONG_MAX;
  h->ctx->in[ 0 ].kind    = TEST_BAM_RESOLVE_IN_KIND;

  TEST_BAM_RESOLVE_OUT( h->ctx, mem    ) = (fd_wksp_t *)h->pack_dcache;
  TEST_BAM_RESOLVE_OUT( h->ctx, chunk0 ) = 0UL;
  TEST_BAM_RESOLVE_OUT( h->ctx, wmark  ) = TEST_DCACHE_CHUNKS-1UL;
  TEST_BAM_RESOLVE_OUT( h->ctx, chunk  ) = 0UL;

#if TEST_BAM_RESOLVE_HAS_REPLAY
  h->ctx->out_replay->mem    = (fd_wksp_t *)h->replay_dcache;
  h->ctx->out_replay->chunk0 = 0UL;
  h->ctx->out_replay->wmark  = TEST_DCACHE_CHUNKS-1UL;
  h->ctx->out_replay->chunk  = 0UL;
#endif

  h->mcaches[ 0 ] = h->pack_mcache;
#if TEST_BAM_RESOLVE_HAS_REPLAY
  h->mcaches[ 1 ] = h->replay_mcache;
#endif
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
  fd_txn_m_t * txnm = fd_chunk_to_laddr( TEST_BAM_RESOLVE_OUT( h->ctx, mem ), TEST_BAM_RESOLVE_OUT( h->ctx, chunk ) );
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

/* Mirrors the tile's completed-slot insert into the parallel rings. */

static void
test_insert_blockhash( test_harness_t * h,
                       fd_txn_m_t *     txnm,
                       ulong            slot ) {
  blockhash_map_t * entry       = h->ctx->blockhash_ring      +(h->ctx->blockhash_ring_idx%BLOCKHASH_RING_LEN);
  blockhash_map_t * nonce_entry = h->ctx->nonce_blockhash_ring+(h->ctx->blockhash_ring_idx%BLOCKHASH_RING_LEN);

  /* See fd_durable_nonce_from_blockhash */
  struct {
    char      tag[13];
    fd_hash_t bh[1];
  } hash_buf[1] = {{ .tag = "DURABLE_NONCE", .bh = { FD_LOAD( fd_hash_t, fd_txn_m_payload( txnm ) ) } }};

  /* Slots intentionally differ from block heights. */
  memcpy( entry->key.b, fd_txn_m_payload( txnm ), 32UL );  fd_sha256_hash( hash_buf, sizeof(hash_buf), nonce_entry->key.b );
  entry->slot         = slot+1000UL;                       nonce_entry->slot         = slot+1000UL;
  entry->block_height = slot;                              nonce_entry->block_height = slot;

  map_ele_insert( h->ctx->blockhash_map,       entry,       h->ctx->blockhash_ring       );
  map_ele_insert( h->ctx->nonce_blockhash_map, nonce_entry, h->ctx->nonce_blockhash_ring );
  h->ctx->blockhash_ring_idx++;
}

/* Observed block height alone cannot distinguish age 150 from age 151, and replay
   may have completed a newer slot on an unrelated fork, so BAM ingress
   defers blockhash validity to the runtime.  Ordinary ingress keeps the
   block-height expiry.  Exercise both around the cutoff and beyond it. */
static void
test_known_hash_height_distance_defers_to_runtime( void ) {
  test_harness_t h[1];
  test_harness_new( h );
  ulong const completed_heights[] = { 160UL, 161UL, 162UL, 171UL, 1000000UL, ULONG_MAX };
  for( ulong source=0UL; source<2UL; source++ ) {
    for( ulong i=0UL; i<sizeof(completed_heights)/sizeof(completed_heights[0]); i++ ) {
      fd_txn_m_t * txnm = test_prepare_bam_txn( h, (uchar)(7UL+source*16UL+i), 0, 0U, 1U, 0 );
      txnm->source_tpu = source ? FD_TXN_M_TPU_SOURCE_BAM : FD_TXN_M_TPU_SOURCE_QUIC;
      test_insert_blockhash( h, txnm, 10UL );
      h->ctx->completed_block_height = completed_heights[i];
      ulong seq = h->seqs[0];
      after_frag( h->ctx, 0UL, 0UL, 0UL, fd_txn_m_realized_footprint( txnm, 1, 0 ), 0UL, 0UL, h->stem );
      int expired = !source && 10UL+151UL<completed_heights[i];
      FD_TEST( h->seqs[0]==seq+(ulong)!expired );
      FD_TEST( txnm->reference_block_height==10UL );
      FD_TEST( !h->ctx->bundle_failed );
    }
  }
  FD_TEST( h->ctx->metrics.blockhash_expired==4UL );
  FD_TEST( pool_free( h->ctx->pool )==4UL );
  test_harness_delete( h );
}

static void
test_no_bank_deser_marker( uchar batch_idx ) {
  test_harness_t h[1];
  test_harness_new( h );

  fd_txn_m_t * txnm = test_prepare_bam_txn( h, 11U, 1, batch_idx, 2U, 1 );
  test_insert_blockhash( h, txnm, 100UL );
  h->ctx->completed_block_height = 100UL;

  after_frag( h->ctx, 0UL, 0UL, 0UL, fd_txn_m_realized_footprint( txnm, 1, 0 ), 0UL, 0UL, h->stem );

  FD_TEST( h->seqs[ 0 ]==1UL );
  FD_TEST( txnm->bam.preprocess_failed );
  FD_TEST( txnm->bam.seq_id    ==123U );
  FD_TEST( txnm->bam.batch_idx ==batch_idx );

  test_harness_delete( h );
}

/* A later member's old observed hash height must not fail it or its siblings.
   Runtime will check each member against the actual execution bank. */
static void
test_old_hash_bam_member_and_siblings_forwarded( void ) {
  for( ulong revert_on_error=0UL; revert_on_error<2UL; revert_on_error++ ) {
    test_harness_t h[1];
    test_harness_new( h );
    h->ctx->completed_block_height = 200UL;

    for( uchar batch_idx=0U; batch_idx<3U; batch_idx++ ) {
      fd_txn_m_t * txnm = test_prepare_bam_txn( h,
                                                (uchar)(21U+batch_idx),
                                                (_Bool)revert_on_error,
                                                batch_idx,
                                                3U,
                                                0 );
      ulong blockhash_height = batch_idx==1U ? 10UL : 200UL;
      test_insert_blockhash( h, txnm, blockhash_height );

      after_frag( h->ctx, 0UL, 0UL, 0UL, fd_txn_m_realized_footprint( txnm, 1, 0 ), 0UL, 0UL, h->stem );

      FD_TEST( h->seqs[ 0 ]==(ulong)batch_idx+1UL );
      FD_TEST( txnm->reference_block_height==blockhash_height );
      FD_TEST( !h->ctx->bundle_failed );
    }
    FD_TEST( h->ctx->metrics.blockhash_expired==0UL );

    test_harness_delete( h );
  }
}

static void
test_unknown_hash_stash_and_bundle_bypass( void ) {
  test_harness_t h[1];
  test_harness_new( h );
  h->ctx->completed_block_height = 1000000UL; /* May belong to an unrelated fork. */

  for( ulong i=0UL; i<8UL; i++ ) {
    fd_txn_m_t * txnm = test_prepare_bam_txn( h, (uchar)(40UL+i), 0, 0U, 1U, 0 );
    txnm->source_tpu = FD_TXN_M_TPU_SOURCE_QUIC;
    after_frag( h->ctx, 0UL, 0UL, 0UL, fd_txn_m_realized_footprint( txnm, 1, 0 ), 0UL, 0UL, h->stem );
    FD_TEST( !h->seqs[0] );
    FD_TEST( pool_free( h->ctx->pool )==4UL-fd_ulong_min( i+1UL, 4UL ) );
  }
  FD_TEST( h->ctx->metrics.stash[ FD_METRICS_ENUM_RESOLVE_STASH_OPERATION_V_INSERTED_IDX ]==8UL );
  FD_TEST( h->ctx->metrics.stash[ FD_METRICS_ENUM_RESOLVE_STASH_OPERATION_V_OVERRUN_IDX ]==4UL );

  /* BAM and Block Engine bundle members bypass the stash. */
  for( ulong source=0UL; source<2UL; source++ ) {
    fd_txn_m_t * txnm = test_prepare_bam_txn( h, (uchar)(60UL+source), source==1UL, 0U, 1U, 0 );
    if( source ) txnm->source_tpu = FD_TXN_M_TPU_SOURCE_QUIC;
    after_frag( h->ctx, 0UL, 0UL, 0UL, fd_txn_m_realized_footprint( txnm, 1, 0 ), 0UL, 0UL, h->stem );
    FD_TEST( h->seqs[0]==source+1UL );
    FD_TEST( txnm->reference_block_height==1000000UL );
    FD_TEST( !pool_free( h->ctx->pool ) );
    FD_TEST( h->ctx->metrics.stash[ FD_METRICS_ENUM_RESOLVE_STASH_OPERATION_V_INSERTED_IDX ]==8UL );
  }
  test_harness_delete( h );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  fd_metrics_register( fd_metrics_join( fd_metrics_new( metrics_scratch, 0UL ) ) );

  test_harness_t h[1];
  test_harness_new( h );
  /* Round-robin routing can send later BAM members through a faster resolver
     and reorder the batch.  Pin BAM traffic while leaving ordinary routing fair. */
  h->ctx->round_robin_cnt = 4UL;
  for( ulong resolver_idx=0UL; resolver_idx<4UL; resolver_idx++ ) {
    h->ctx->round_robin_idx = resolver_idx;
    FD_TEST( before_frag( h->ctx, 0UL, 2UL, 1UL )==(resolver_idx!=0UL) );
    FD_TEST( before_frag( h->ctx, 0UL, 2UL, 0UL )==(resolver_idx!=2UL) );
  }
  test_harness_delete( h );

  test_known_hash_height_distance_defers_to_runtime();
  test_old_hash_bam_member_and_siblings_forwarded();
  test_no_bank_deser_marker( 0U );
  test_no_bank_deser_marker( 1U );
  test_unknown_hash_stash_and_bundle_bypass();

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
