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

/* Drives after_frag directly on one unreliable output, so publishing
   never touches credits. */
typedef struct {
  fd_verify_ctx_t   ctx[1];
  uchar             dcache[ FD_TPU_PARSED_MTU ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_frag_meta_t    mcache[ 16UL ] __attribute__((aligned(alignof(fd_frag_meta_t))));
  fd_frag_meta_t *  mcaches[ 1 ];
  ulong             seqs[ 1 ];
  ulong             depths[ 1 ];
  int               out_reliable[ 1 ];
  fd_stem_context_t stem[1];
} test_env_t;

static fd_txn_m_t *
test_env_init( test_env_t * env ) {
  fd_memset( env, 0, sizeof(test_env_t) );
  env->ctx->out_mem   = (fd_wksp_t *)env->dcache;
  env->ctx->out_wmark = sizeof(env->dcache)/FD_CHUNK_SZ - 1UL;
  env->mcaches[ 0 ]   = env->mcache;
  env->depths [ 0 ]   = 16UL;
  *env->stem = (fd_stem_context_t){ .mcaches=env->mcaches, .seqs=env->seqs, .depths=env->depths, .out_reliable=env->out_reliable };
  return (fd_txn_m_t *)fd_chunk_to_laddr( env->ctx->out_mem, env->ctx->out_chunk );
}

/* Publishing a verify failure directly races pack's batch result and produces
   two terminal outcomes.  Verify must emit only a preprocessing marker. */
static void
test_bam_atomic_verify_failure_result_owner( void ) {
  test_env_t env[1];

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
    fd_txn_m_t * txnm = test_env_init( env );
    env->ctx->in_kind[ IN_IDX_BAM ] = IN_KIND_BAM;
    *txnm = (fd_txn_m_t) {
      .payload_sz = 0U,
      .source_tpu = FD_TXN_M_TPU_SOURCE_BAM,
      .bam = {
        .bundle_id         = cases[ case_idx ].revert_on_error ? 78UL : 0UL,
        .bundle_txn_cnt    = (cases[ case_idx ].revert_on_error && !cases[ case_idx ].batch_idx) ? cases[ case_idx ].txn_cnt : 0UL,
        .max_schedule_slot = 100UL,
        .seq_id            = 77U,
        .txn_cnt           = cases[ case_idx ].txn_cnt,
        .batch_idx         = cases[ case_idx ].batch_idx,
        .revert_on_error   = cases[ case_idx ].revert_on_error,
      },
    };

    after_frag( env->ctx, IN_IDX_BAM, 0UL, 0UL, sizeof(fd_txn_m_t), 0UL, 0UL, env->stem );

    FD_TEST( env->seqs[ 0 ]==1UL );
    FD_TEST( txnm->bam.preprocess_failed );
    FD_TEST( txnm->bam.seq_id    ==77U );
    FD_TEST( txnm->bam.batch_idx ==cases[ case_idx ].batch_idx );
    FD_TEST( txnm->txn_t_sz      ==fd_txn_footprint( 0UL, 0UL ) );
  }

  /* A failed member fails its later peers until the seq_id is resent
     as a new batch (batch_idx back to zero).  Each frag arrives at the
     out_chunk the previous publish advanced to. */
  test_env_init( env );
  for( ulong i=0UL; i<3UL; i++ ) {
    uchar batch_idx = (uchar)( i==1UL );
    fd_txn_m_t * txnm = (fd_txn_m_t *)fd_chunk_to_laddr( env->ctx->out_mem, env->ctx->out_chunk );
    *txnm = (fd_txn_m_t) {
      .payload_sz = 0U,
      .source_tpu = FD_TXN_M_TPU_SOURCE_BAM,
      .bam = {
        .bundle_id         = 89UL,
        .bundle_txn_cnt    = batch_idx ? 0UL : 2UL,
        .max_schedule_slot = 100UL,
        .seq_id            = 88U,
        .txn_cnt           = 2U,
        .batch_idx         = batch_idx,
        .revert_on_error   = 1U,
      },
    };

    after_frag( env->ctx, IN_IDX_BAM, 0UL, 0UL, sizeof(fd_txn_m_t), 0UL, 0UL, env->stem );

    FD_TEST( env->seqs[ 0 ]==1UL+(ulong)( i==2UL ) );
    FD_TEST( env->ctx->bundle_failed );
    FD_TEST( env->ctx->metrics.verify_tile_result[ FD_METRICS_ENUM_VERIFY_TILE_RESULT_V_BUNDLE_PEER_FAILURE_IDX ]==(ulong)( i>0UL ) );
  }

  /* A non-BAM frag keeps upstream's path: this empty one fails to parse
     and is dropped, with no failure marker. */
  *(fd_txn_m_t *)fd_chunk_to_laddr( env->ctx->out_mem, env->ctx->out_chunk ) = (fd_txn_m_t){ .source_tpu = FD_TXN_M_TPU_SOURCE_QUIC };
  after_frag( env->ctx, IN_IDX_QUIC, 0UL, 0UL, sizeof(fd_txn_m_t), 0UL, 0UL, env->stem );
  FD_TEST( env->seqs[ 0 ]==2UL );
}

FD_IMPORT_BINARY( test_sample_vote, "src/disco/pack/sample_vote.bin" );

/* A BAM txn that fails sigverify is forwarded only as a failure marker
   for pack.  Consumers of the verified stream, e.g. tower's vote
   counting, trust the parsed txn, so it must not survive.  Uses a real
   vote payload with one signature bit flipped, then the intact vote. */
static void
test_bam_sigverify_failure_drops_parsed_txn( void ) {
  static fd_sha512_t sha[ FD_TXN_SIG_MAX ];
  test_env_t env[1];

  FD_TEST( test_sample_vote_sz<=FD_TPU_MTU );

  void * cache_mem = test_malloc( fd_ed25519_cache_align(), fd_ed25519_cache_footprint( FD_VERIFY_ED25519_CACHE_ENT_CNT ) );
  fd_ed25519_cache_t * cache = fd_ed25519_cache_join( fd_ed25519_cache_new( cache_mem, FD_VERIFY_ED25519_CACHE_ENT_CNT, 0UL ) );
  FD_TEST( cache );

  /* Cover failure before admission, repeated valid signatures warming
     the signer cache, then failure while that signer is cached. */
  int const bad_sigs[] = { 1, 0, 0, 0, 1 };
  for( ulong case_idx=0UL; case_idx<sizeof(bad_sigs)/sizeof(bad_sigs[0]); case_idx++ ) {
    int bad_sig = bad_sigs[ case_idx ];
    fd_txn_m_t * txnm = test_env_init( env );
    fd_verify_ctx_t * ctx = env->ctx;
    ctx->ed25519_cache = cache;
    ctx->in_kind[ IN_IDX_BAM ] = IN_KIND_BAM;
    for( ulong i=0UL; i<FD_TXN_SIG_MAX; i++ ) FD_TEST( (ctx->sha[ i ] = fd_sha512_join( fd_sha512_new( &sha[ i ] ) )) );

    /* As the bam tile publishes it: payload plus its own parse. */
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

    after_frag( ctx, IN_IDX_BAM, 0UL, 0UL, fd_txn_m_realized_footprint( txnm, 1, 0 ), 0UL, 0UL, env->stem );

    FD_TEST( env->seqs[ 0 ]==1UL );
    FD_TEST( txnm->payload_sz==test_sample_vote_sz );
    FD_TEST( txnm->bam.seq_id==99U );
    if( bad_sig ) {
      FD_TEST( ctx->metrics.verify_tile_result[ FD_METRICS_ENUM_VERIFY_TILE_RESULT_V_VERIFY_FAILURE_IDX ]==1UL );
      FD_TEST( txnm->bam.preprocess_failed );
      FD_TEST( txnm->txn_t_sz==fd_txn_footprint( 0UL, 0UL ) );
      FD_TEST( env->mcache[ 0 ].sz==fd_txn_m_realized_footprint( txnm, 1, 0 ) );
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

  /* Non-BAM frags keep upstream's path, which parses the payload itself. */
  struct { ulong bundle_id; ulong success_cnt; ulong dedup_cnt; } const non_bam[] = {
    { 0UL, 1UL, 0UL }, /* a QUIC vote is verified and published */
    { 0UL, 1UL, 1UL }, /* its resend is dropped by the HA dedup tcache */
    { 5UL, 2UL, 1UL }, /* a Block Engine bundle member skips that tcache */
  };
  fd_tcache_t * tcache = fd_tcache_join( fd_tcache_new( test_malloc( fd_tcache_align(), fd_tcache_footprint( TCACHE_DEPTH, 0UL ) ), TCACHE_DEPTH, 0UL ) );
  test_env_init( env );
  fd_verify_ctx_t * ctx = env->ctx;
  ctx->ed25519_cache  = cache;
  ctx->tcache_depth   = fd_tcache_depth       ( tcache );
  ctx->tcache_map_cnt = fd_tcache_map_cnt     ( tcache );
  ctx->tcache_sync    = fd_tcache_oldest_laddr( tcache );
  ctx->tcache_ring    = fd_tcache_ring_laddr  ( tcache );
  ctx->tcache_map     = fd_tcache_map_laddr   ( tcache );
  for( ulong i=0UL; i<FD_TXN_SIG_MAX; i++ ) FD_TEST( (ctx->sha[ i ] = fd_sha512_join( fd_sha512_new( &sha[ i ] ) )) );
  for( ulong i=0UL; i<3UL; i++ ) {
    ulong        chunk = ctx->out_chunk;
    fd_txn_m_t * txnm  = (fd_txn_m_t *)fd_chunk_to_laddr( ctx->out_mem, chunk );
    *txnm = (fd_txn_m_t) {
      .payload_sz   = (ushort)test_sample_vote_sz,
      .source_tpu   = non_bam[ i ].bundle_id ? FD_TXN_M_TPU_SOURCE_BUNDLE : FD_TXN_M_TPU_SOURCE_QUIC,
      .block_engine = { .bundle_id = non_bam[ i ].bundle_id, .bundle_txn_cnt = (ulong)!!non_bam[ i ].bundle_id },
    };
    fd_memcpy( fd_txn_m_payload( txnm ), test_sample_vote, test_sample_vote_sz );

    after_frag( ctx, IN_IDX_QUIC, 0UL, 0UL, sizeof(fd_txn_m_t)+test_sample_vote_sz, 0UL, 0UL, env->stem );

    FD_TEST( ctx->metrics.verify_tile_result[ FD_METRICS_ENUM_VERIFY_TILE_RESULT_V_SUCCESS_IDX       ]==non_bam[ i ].success_cnt );
    FD_TEST( ctx->metrics.verify_tile_result[ FD_METRICS_ENUM_VERIFY_TILE_RESULT_V_DEDUP_FAILURE_IDX ]==non_bam[ i ].dedup_cnt   );
    FD_TEST( env->seqs[ 0 ]==non_bam[ i ].success_cnt );
    FD_TEST( (ctx->out_chunk!=chunk)==(i!=1UL) );
    if( i!=1UL ) FD_TEST( txnm->txn_t_sz && env->mcache[ i/2UL ].sz==fd_txn_m_realized_footprint( txnm, 1, 0 ) );
  }
  FD_TEST( ctx->bundle_id==5UL );
  for( ulong i=0UL; i<FD_TXN_SIG_MAX; i++ ) FD_TEST( fd_sha512_delete( fd_sha512_leave( ctx->sha[ i ] ) ) );

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
