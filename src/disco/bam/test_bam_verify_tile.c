/* test_bam_verify_tile covers the verify tile's bam_verif input: it is
   served only by verify:0, and a BAM txn that fails is forwarded as a
   preprocess_failed marker for pack instead of being dropped.  Uses
   libc malloc like test_verify_tile. */

#define FD_TILE_TEST
#include "../verify/fd_verify_tile.c"
#include "../topo/fd_topob.h"
#include <stdlib.h>

#define TCACHE_DEPTH (128UL)
#define LINK_DEPTH   (16UL)

#define TEST_ALLOC_MAX (16UL)
static void * test_allocs[ TEST_ALLOC_MAX ];
static ulong  test_alloc_cnt;

static void *
test_malloc( ulong align, ulong sz ) {
  FD_TEST( test_alloc_cnt<TEST_ALLOC_MAX );
  void * p = aligned_alloc( align, fd_ulong_align_up( sz, align ) );
  FD_TEST( p );
  test_allocs[ test_alloc_cnt++ ] = p;
  return p;
}

static void
test_free_all( void ) {
  while( test_alloc_cnt ) free( test_allocs[ --test_alloc_cnt ] );
}

static void
mock_link_create( fd_topo_t *  topo,
                  char const * name ) {
  fd_topo_link_t * link = fd_topob_link( topo, name, "wksp", LINK_DEPTH, FD_TPU_PARSED_MTU, 1UL );
  ulong data_sz = fd_dcache_req_data_sz( FD_TPU_PARSED_MTU, LINK_DEPTH, 1UL, 1 );
  link->mcache  = fd_mcache_join( fd_mcache_new( test_malloc( fd_mcache_align(), fd_mcache_footprint( LINK_DEPTH, 0UL ) ), LINK_DEPTH, 0UL, 0UL ) );
  link->dcache  = fd_dcache_join( fd_dcache_new( test_malloc( fd_dcache_align(), fd_dcache_footprint( data_sz, 0UL ) ), data_sz, 0UL ) );
}

/* The BAM link is declared first so its in idx differs from its
   IN_KIND. */
#define IN_IDX_BAM  0
#define IN_IDX_QUIC 1

static void
test_bam_load_balance( void ) {
  fd_topo_t * topo = fd_topob_new( test_malloc( alignof(fd_topo_t), sizeof(fd_topo_t) ), "verify-bam-test" );
  fd_topob_wksp( topo, "wksp" )->wksp = NULL;
  fd_topo_tile_t * tile = fd_topob_tile( topo, "verify", "wksp", "wksp", 0UL, 0, 0, 0, 0 );
  tile->verify.tcache_depth = TCACHE_DEPTH;
  topo->objs[ tile->tile_obj_id ].offset = (ulong)test_malloc( scratch_align(), scratch_footprint( tile ) );
  mock_link_create( topo, "bam_verif"   );
  mock_link_create( topo, "quic_verify" );
  fd_topob_tile_in( topo, "verify", 0UL, "wksp", "bam_verif",   0UL, 0, 1 );
  fd_topob_tile_in( topo, "verify", 0UL, "wksp", "quic_verify", 0UL, 0, 1 );

  privileged_init( topo, tile );
  unprivileged_init( topo, tile );
  fd_verify_ctx_t * ctx = fd_topo_obj_laddr( topo, tile->tile_obj_id );
  FD_TEST( ctx->in_kind[ IN_IDX_BAM  ]==IN_KIND_BAM  );
  FD_TEST( ctx->in_kind[ IN_IDX_QUIC ]==IN_KIND_QUIC );

  /* BAM batches must stay ordered, so verify:0 takes every BAM frag
     whatever its seq or sig and the other verify tiles take none. */
  for( ulong rr_cnt=1UL; rr_cnt<=4UL; rr_cnt+=3UL ) {
    for( ulong rr_idx=0UL; rr_idx<rr_cnt; rr_idx++ ) {
      ctx->round_robin_cnt = rr_cnt;
      ctx->round_robin_idx = rr_idx;
      for( ulong seq=0UL; seq<3UL; seq++ ) {
        for( ulong sig=0UL; sig<2UL; sig++ ) {
          FD_TEST( before_frag( ctx, IN_IDX_BAM, seq, sig )==(rr_idx!=0UL) );
        }
      }
    }
  }

  test_free_all();
}

/* Publishing a verify failure directly races pack's batch result and produces
   two terminal outcomes.  Verify must emit only a preprocessing marker. */
static void
test_bam_atomic_verify_failure_result_owner( void ) {
  fd_verify_ctx_t ctx[1];
  uchar           verify_dcache[ FD_TPU_PARSED_MTU ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_frag_meta_t  verify_mcache[ 16UL ] __attribute__((aligned(alignof(fd_frag_meta_t))));

  fd_frag_meta_t * mcaches[ 1 ]      = { verify_mcache };
  ulong            seqs[ 1 ]         = { 0UL };
  ulong            depths[ 1 ]       = { 16UL };
  ulong            cr_avail[ 1 ]     = { ULONG_MAX };
  ulong            min_cr_avail      = ULONG_MAX;
  int              out_reliable[ 1 ] = { 0 };
  fd_stem_context_t stem = {
    .mcaches             = mcaches,
    .seqs                = seqs,
    .depths              = depths,
    .cr_avail            = cr_avail,
    .min_cr_avail        = &min_cr_avail,
    .cr_decrement_amount = 0UL,
    .out_reliable        = out_reliable,
  };

  struct {
    _Bool revert_on_error;
    uchar batch_idx;
    uchar txn_cnt;
  } cases[] = {
    { 0, 0U, 1U },
    { 1, 0U, 2U },
    { 1, 1U, 2U },
  };

  for( ulong case_idx=0UL; case_idx<sizeof(cases)/sizeof(cases[0]); case_idx++ ) {
    fd_memset( ctx,           0, sizeof(fd_verify_ctx_t) );
    fd_memset( verify_dcache, 0, sizeof(verify_dcache) );
    fd_memset( verify_mcache, 0, sizeof(verify_mcache) );
    seqs[ 0 ] = 0UL;

    ctx->out_mem    = (fd_wksp_t *)verify_dcache;
    ctx->out_chunk0 = 0UL;
    ctx->out_wmark  = sizeof(verify_dcache)/FD_CHUNK_SZ - 1UL;
    ctx->out_chunk  = 0UL;
    ctx->in_kind[ IN_IDX_BAM ] = IN_KIND_BAM;

    fd_txn_m_t * txnm = (fd_txn_m_t *)fd_chunk_to_laddr( ctx->out_mem, ctx->out_chunk );
    *txnm = (fd_txn_m_t) {
      .payload_sz = 0U,
      .source_tpu = FD_TXN_M_TPU_SOURCE_BAM,
      .block_engine = {
        .bundle_id      = cases[ case_idx ].revert_on_error ? 78UL : 0UL,
        .bundle_txn_cnt = (cases[ case_idx ].revert_on_error && !cases[ case_idx ].batch_idx) ? cases[ case_idx ].txn_cnt : 0UL,
      },
      .bam = {
        .max_schedule_slot = 100UL,
        .seq_id            = 77U,
        .txn_cnt           = cases[ case_idx ].txn_cnt,
        .batch_idx         = cases[ case_idx ].batch_idx,
        .revert_on_error   = cases[ case_idx ].revert_on_error,
      },
    };

    after_frag( ctx, IN_IDX_BAM, 0UL, 0UL, sizeof(fd_txn_m_t), 0UL, 0UL, &stem );

    FD_TEST( seqs[ 0 ]==1UL );
    FD_TEST( txnm->bam.preprocess_failed );
    FD_TEST( txnm->bam.seq_id    ==77U );
    FD_TEST( txnm->bam.batch_idx ==cases[ case_idx ].batch_idx );
    FD_TEST( txnm->txn_t_sz      ==fd_txn_footprint( 0UL, 0UL ) );
  }

  fd_memset( ctx,           0, sizeof(fd_verify_ctx_t) );
  fd_memset( verify_dcache, 0, sizeof(verify_dcache) );
  fd_memset( verify_mcache, 0, sizeof(verify_mcache) );
  seqs[ 0 ] = 0UL;

  ctx->out_mem    = (fd_wksp_t *)verify_dcache;
  ctx->out_chunk0 = 0UL;
  ctx->out_wmark  = sizeof(verify_dcache)/FD_CHUNK_SZ - 1UL;
  ctx->out_chunk  = 0UL;

  fd_txn_m_t * txnm = (fd_txn_m_t *)fd_chunk_to_laddr( ctx->out_mem, ctx->out_chunk );
  *txnm = (fd_txn_m_t) {
    .payload_sz = 0U,
    .source_tpu = FD_TXN_M_TPU_SOURCE_BAM,
    .bam = {
      .max_schedule_slot = 100UL,
      .seq_id            = 88U,
      .txn_cnt           = 2U,
      .batch_idx         = 0U,
      .revert_on_error   = 0U,
    },
  };

  after_frag( ctx, IN_IDX_BAM, 0UL, 0UL, sizeof(fd_txn_m_t), 0UL, 0UL, &stem );

  FD_TEST( seqs[ 0 ]==1UL );
  FD_TEST( ctx->bundle_failed );

  *txnm = (fd_txn_m_t) {
    .payload_sz = 0U,
    .source_tpu = FD_TXN_M_TPU_SOURCE_BAM,
    .bam = {
      .max_schedule_slot = 100UL,
      .seq_id            = 88U,
      .txn_cnt           = 2U,
      .batch_idx         = 1U,
      .revert_on_error   = 0U,
    },
  };

  after_frag( ctx, IN_IDX_BAM, 0UL, 0UL, sizeof(fd_txn_m_t), 0UL, 0UL, &stem );

  FD_TEST( seqs[ 0 ]==1UL );
}

FD_IMPORT_BINARY( test_sample_vote, "src/disco/pack/sample_vote.bin" );

/* A BAM txn that fails sigverify is forwarded only as a failure marker
   for pack.  Consumers of the verified stream, e.g. tower's vote
   counting, trust the parsed txn, so it must not survive.  Uses a real
   vote payload with one signature bit flipped, then the intact vote. */
static void
test_bam_sigverify_failure_drops_parsed_txn( void ) {
  static fd_sha512_t sha[ FD_TXN_SIG_MAX ];
  fd_verify_ctx_t ctx[1];
  uchar           verify_dcache[ FD_TPU_PARSED_MTU ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_frag_meta_t  verify_mcache[ 16UL ] __attribute__((aligned(alignof(fd_frag_meta_t))));

  fd_frag_meta_t * mcaches[ 1 ]      = { verify_mcache };
  ulong            seqs[ 1 ]         = { 0UL };
  ulong            depths[ 1 ]       = { 16UL };
  ulong            cr_avail[ 1 ]     = { ULONG_MAX };
  ulong            min_cr_avail      = ULONG_MAX;
  int              out_reliable[ 1 ] = { 0 };
  fd_stem_context_t stem = {
    .mcaches             = mcaches,
    .seqs                = seqs,
    .depths              = depths,
    .cr_avail            = cr_avail,
    .min_cr_avail        = &min_cr_avail,
    .cr_decrement_amount = 0UL,
    .out_reliable        = out_reliable,
  };

  FD_TEST( test_sample_vote_sz<=FD_TPU_MTU );

  void * cache_mem = test_malloc( fd_ed25519_cache_align(), fd_ed25519_cache_footprint( FD_VERIFY_ED25519_CACHE_ENT_CNT ) );
  fd_ed25519_cache_t * cache = fd_ed25519_cache_join( fd_ed25519_cache_new( cache_mem, FD_VERIFY_ED25519_CACHE_ENT_CNT, 0UL ) );
  FD_TEST( cache );

  /* Cover failure before admission, repeated valid signatures warming
     the signer cache, then failure while that signer is cached. */
  int const bad_sigs[] = { 1, 0, 0, 0, 1 };
  for( ulong case_idx=0UL; case_idx<sizeof(bad_sigs)/sizeof(bad_sigs[0]); case_idx++ ) {
    int bad_sig = bad_sigs[ case_idx ];
    fd_memset( ctx,           0, sizeof(fd_verify_ctx_t) );
    ctx->ed25519_cache = cache;
    fd_memset( verify_dcache, 0, sizeof(verify_dcache) );
    fd_memset( verify_mcache, 0, sizeof(verify_mcache) );
    seqs[ 0 ] = 0UL;

    for( ulong i=0UL; i<FD_TXN_SIG_MAX; i++ ) FD_TEST( (ctx->sha[ i ] = fd_sha512_join( fd_sha512_new( &sha[ i ] ) )) );
    ctx->out_mem    = (fd_wksp_t *)verify_dcache;
    ctx->out_chunk0 = 0UL;
    ctx->out_wmark  = sizeof(verify_dcache)/FD_CHUNK_SZ - 1UL;
    ctx->out_chunk  = 0UL;
    ctx->in_kind[ IN_IDX_BAM ] = IN_KIND_BAM;

    /* As the bam tile publishes it: payload plus its own parse. */
    fd_txn_m_t * txnm = (fd_txn_m_t *)fd_chunk_to_laddr( ctx->out_mem, ctx->out_chunk );
    *txnm = (fd_txn_m_t) {
      .payload_sz = (ushort)test_sample_vote_sz,
      .source_tpu = FD_TXN_M_TPU_SOURCE_BAM,
      .bam = {
        .max_schedule_slot = 100UL,
        .seq_id            = 99U,
        .txn_cnt           = 1U,
      },
    };
    uchar *    payload = fd_txn_m_payload( txnm );
    fd_txn_t * txnt    = fd_txn_m_txn_t( txnm );
    fd_memcpy( payload, test_sample_vote, test_sample_vote_sz );
    txnm->txn_t_sz = (ushort)fd_txn_parse( payload, txnm->payload_sz, txnt, NULL );
    FD_TEST( txnm->txn_t_sz );
    FD_TEST( fd_txn_is_simple_vote_transaction( txnt, payload ) );
    if( bad_sig ) payload[ txnt->signature_off ] ^= (uchar)1;

    after_frag( ctx, IN_IDX_BAM, 0UL, 0UL, fd_txn_m_realized_footprint( txnm, 1, 0 ), 0UL, 0UL, &stem );

    FD_TEST( seqs[ 0 ]==1UL );
    FD_TEST( txnm->payload_sz==test_sample_vote_sz );
    FD_TEST( txnm->bam.seq_id==99U );
    if( bad_sig ) {
      FD_TEST( ctx->metrics.verify_tile_result[ FD_METRICS_ENUM_VERIFY_TILE_RESULT_V_VERIFY_FAILURE_IDX ]==1UL );
      FD_TEST( txnm->bam.preprocess_failed );
      FD_TEST( txnm->txn_t_sz==fd_txn_footprint( 0UL, 0UL ) );
      FD_TEST( verify_mcache[ 0 ].sz==fd_txn_m_realized_footprint( txnm, 1, 0 ) );
      FD_TEST( !fd_txn_is_simple_vote_transaction( fd_txn_m_txn_t_const( txnm ), fd_txn_m_payload_const( txnm ) ) );
    } else {
      FD_TEST( ctx->metrics.verify_tile_result[ FD_METRICS_ENUM_VERIFY_TILE_RESULT_V_SUCCESS_IDX ]==1UL );
      FD_TEST( !txnm->bam.preprocess_failed );
      FD_TEST( fd_txn_is_simple_vote_transaction( fd_txn_m_txn_t_const( txnm ), fd_txn_m_payload_const( txnm ) ) );
    }

    for( ulong i=0UL; i<FD_TXN_SIG_MAX; i++ ) FD_TEST( fd_sha512_delete( fd_sha512_leave( ctx->sha[ i ] ) ) );
  }

  FD_TEST( fd_ed25519_cache_insert_cnt( cache )>0UL );
  FD_TEST( fd_ed25519_cache_hit_cnt( cache )>0UL );
  FD_TEST( fd_ed25519_cache_delete( fd_ed25519_cache_leave( cache ) )==cache_mem );
  test_free_all();
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  /* Sandbox policy is covered by test_verify_tile. */
  (void)populate_allowed_seccomp;
  (void)populate_allowed_fds;

  test_bam_load_balance();
  test_bam_atomic_verify_failure_result_owner();
  test_bam_sigverify_failure_drops_parsed_txn();

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
