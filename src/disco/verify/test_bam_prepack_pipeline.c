#define FD_TILE_TEST

#define STEM_NAME               verify_tile_stem
#define scratch_align            verify_scratch_align
#define scratch_footprint        verify_scratch_footprint
#define metrics_write            verify_metrics_write
#define before_frag              verify_before_frag
#define during_frag              verify_during_frag
#define after_frag               verify_after_frag
#define privileged_init          verify_privileged_init
#define unprivileged_init        verify_unprivileged_init
#define populate_allowed_seccomp verify_populate_allowed_seccomp
#define populate_allowed_fds     verify_populate_allowed_fds
#include "fd_verify_tile.c"
#undef scratch_align
#undef scratch_footprint
#undef metrics_write
#undef before_frag
#undef during_frag
#undef after_frag
#undef privileged_init
#undef unprivileged_init
#undef populate_allowed_seccomp
#undef populate_allowed_fds
#undef STEM_NAME
#undef IN_KIND_QUIC
#undef IN_KIND_BUNDLE
#undef IN_KIND_GOSSIP
#undef IN_KIND_SEND
#undef IN_KIND_BAM

#define STEM_NAME               dedup_tile_stem
#define scratch_align            dedup_scratch_align
#define scratch_footprint        dedup_scratch_footprint
#define metrics_write            dedup_metrics_write
#define during_frag              dedup_during_frag
#define after_frag               dedup_after_frag
#define privileged_init          dedup_privileged_init
#define unprivileged_init        dedup_unprivileged_init
#define populate_allowed_seccomp dedup_populate_allowed_seccomp
#define populate_allowed_fds     dedup_populate_allowed_fds
#include "../dedup/fd_dedup_tile.c"
#undef scratch_align
#undef scratch_footprint
#undef metrics_write
#undef during_frag
#undef after_frag
#undef privileged_init
#undef unprivileged_init
#undef populate_allowed_seccomp
#undef populate_allowed_fds
#undef STEM_NAME
#undef IN_KIND_GOSSIP
#undef IN_KIND_VERIFY
#undef IN_KIND_EXECUTED_TXN

#include "../topo/fd_topob.h"
#include <stdlib.h>

FD_IMPORT_BINARY( sample_vote, "src/disco/pack/sample_vote.bin" );

#define TCACHE_DEPTH (128UL)
#define LINK_DEPTH   (16UL)

#define VERIFY_IN_IDX_BAM  (0UL)
#define VERIFY_IN_IDX_QUIC (1UL)
#define DEDUP_IN_IDX_VERIFY (0UL)

#define TEST_ALLOC_MAX (64UL)
static void * test_allocs[ TEST_ALLOC_MAX ];
static ulong  test_alloc_cnt;

static void *
test_malloc( ulong align,
             ulong sz ) {
  FD_TEST( test_alloc_cnt<TEST_ALLOC_MAX );
  void * p = aligned_alloc( align, sz );
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
                  char const * wksp_name,
                  char const * name ) {
  fd_topo_link_t * link = fd_topob_link( topo, name, wksp_name, LINK_DEPTH, FD_TPU_PARSED_MTU, 1UL );
  ulong data_sz = fd_dcache_req_data_sz( link->mtu, LINK_DEPTH, 1UL, 1 );
  link->mcache  = fd_mcache_join( fd_mcache_new( test_malloc( fd_mcache_align(), fd_mcache_footprint( LINK_DEPTH, 0UL ) ), LINK_DEPTH, 0UL, 0UL ) );
  link->dcache  = fd_dcache_join( fd_dcache_new( test_malloc( fd_dcache_align(), fd_dcache_footprint( data_sz, 0UL ) ), data_sz, 0UL ) );
  topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ].wksp = (fd_wksp_t *)link->dcache;
}

static fd_topo_t *
mock_topo_create( void ) {
  fd_topo_t * topo = fd_topob_new( test_malloc( alignof(fd_topo_t), sizeof(fd_topo_t) ), "bam-prepack-pipeline" );

  fd_topo_wksp_t * tile_wksp = fd_topob_wksp( topo, "tilewksp" );
  tile_wksp->wksp = NULL;
  fd_topob_wksp( topo, "bamwksp"    );
  fd_topob_wksp( topo, "quicwksp"   );
  fd_topob_wksp( topo, "verifywksp" );
  fd_topob_wksp( topo, "dedupwksp"  );

  fd_topo_tile_t * verify = fd_topob_tile( topo, "verify", "tilewksp", "tilewksp", 0UL, 0, 0 );
  verify->verify.tcache_depth = TCACHE_DEPTH;
  fd_verify_ctx_t * verify_ctx = test_malloc( verify_scratch_align(), verify_scratch_footprint( verify ) + 512UL );
  topo->objs[ verify->tile_obj_id ].offset = (ulong)verify_ctx;

  fd_topo_tile_t * dedup = fd_topob_tile( topo, "dedup", "tilewksp", "tilewksp", 0UL, 0, 0 );
  dedup->dedup.tcache_depth = TCACHE_DEPTH;
  fd_dedup_ctx_t * dedup_ctx = test_malloc( dedup_scratch_align(), dedup_scratch_footprint( dedup ) + 512UL );
  topo->objs[ dedup->tile_obj_id ].offset = (ulong)dedup_ctx;

  mock_link_create( topo, "bamwksp",    "bam_verif"    );
  mock_link_create( topo, "quicwksp",   "quic_verify"  );
  mock_link_create( topo, "verifywksp", "verify_dedup" );
  mock_link_create( topo, "dedupwksp",  "dedup_resolv" );

  fd_topob_tile_in(  topo, "verify", 0UL, "tilewksp", "bam_verif",    0UL, FD_TOPOB_RELIABLE, FD_TOPOB_POLLED );
  fd_topob_tile_in(  topo, "verify", 0UL, "tilewksp", "quic_verify",  0UL, FD_TOPOB_RELIABLE, FD_TOPOB_POLLED );
  fd_topob_tile_out( topo, "verify", 0UL,         "verify_dedup", 0UL );

  fd_topob_tile_in(  topo, "dedup",  0UL, "tilewksp", "verify_dedup", 0UL, FD_TOPOB_RELIABLE, FD_TOPOB_POLLED );
  fd_topob_tile_out( topo, "dedup",  0UL,         "dedup_resolv", 0UL );

  return topo;
}

typedef struct {
  fd_frag_meta_t *  mcache;
  fd_frag_meta_t *  mcaches[ 1 ];
  ulong             seqs[ 1 ];
  ulong             depths[ 1 ];
  ulong             cr_avail[ 1 ];
  ulong             min_cr_avail;
  fd_stem_context_t stem;
} test_stem_t;

static void
test_stem_init( test_stem_t *    stem,
                fd_frag_meta_t * mcache,
                ulong            depth ) {
  stem->mcache          = mcache;
  stem->mcaches[ 0 ]    = mcache;
  stem->seqs[ 0 ]       = 0UL;
  stem->depths[ 0 ]     = depth;
  stem->cr_avail[ 0 ]   = ULONG_MAX;
  stem->min_cr_avail    = ULONG_MAX;
  stem->stem = (fd_stem_context_t) {
    .mcaches             = stem->mcaches,
    .seqs                = stem->seqs,
    .depths              = stem->depths,
    .cr_avail            = stem->cr_avail,
    .min_cr_avail        = &stem->min_cr_avail,
    .cr_decrement_amount = 0UL,
  };
}

static void
test_dedup_init( fd_topo_t *      topo,
                 fd_topo_tile_t * tile ) {
  fd_dedup_ctx_t * ctx = fd_topo_obj_laddr( topo, tile->tile_obj_id );
  fd_tcache_t * tcache = fd_tcache_join( fd_tcache_new( test_malloc( fd_tcache_align(), fd_tcache_footprint( tile->dedup.tcache_depth, 0UL ) ),
                                                        tile->dedup.tcache_depth,
                                                        0UL ) );
  FD_TEST( tcache );

  ctx->bundle_failed = 0;
  ctx->bundle_id     = 0UL;
  ctx->bundle_idx    = 0UL;
  fd_memset( &ctx->metrics, 0, sizeof(ctx->metrics) );

  ctx->tcache_depth   = fd_tcache_depth       ( tcache );
  ctx->tcache_map_cnt = fd_tcache_map_cnt     ( tcache );
  ctx->tcache_sync    = fd_tcache_oldest_laddr( tcache );
  ctx->tcache_ring    = fd_tcache_ring_laddr  ( tcache );
  ctx->tcache_map     = fd_tcache_map_laddr   ( tcache );

  FD_TEST( tile->in_cnt==1UL );
  fd_topo_link_t * in_link = &topo->links[ tile->in_link_id[ 0 ] ];
  fd_topo_wksp_t * in_wksp = &topo->workspaces[ topo->objs[ in_link->dcache_obj_id ].wksp_id ];
  ctx->in[ 0 ].mem    = in_wksp->wksp;
  ctx->in[ 0 ].mtu    = in_link->mtu;
  ctx->in[ 0 ].chunk0 = fd_dcache_compact_chunk0( ctx->in[ 0 ].mem, in_link->dcache );
  ctx->in[ 0 ].wmark  = fd_dcache_compact_wmark ( ctx->in[ 0 ].mem, in_link->dcache, in_link->mtu );
  ctx->in_kind[ 0 ]   = 1UL; /* IN_KIND_VERIFY */

  fd_topo_link_t * out_link = &topo->links[ tile->out_link_id[ 0 ] ];
  ctx->out_mem    = topo->workspaces[ topo->objs[ out_link->dcache_obj_id ].wksp_id ].wksp;
  ctx->out_chunk0 = fd_dcache_compact_chunk0( ctx->out_mem, out_link->dcache );
  ctx->out_wmark  = fd_dcache_compact_wmark ( ctx->out_mem, out_link->dcache, out_link->mtu );
  ctx->out_chunk  = ctx->out_chunk0;
}

static void
test_seccomp( void ) {
  void (* dedup_init_fn )( fd_topo_t *, fd_topo_tile_t * ) = dedup_unprivileged_init;
  void (* verify_metrics_fn )( fd_verify_ctx_t * ) = verify_metrics_write;
  void (* dedup_metrics_fn  )( fd_dedup_ctx_t * )  = dedup_metrics_write;
  FD_COMPILER_FORGET( dedup_init_fn );
  FD_COMPILER_FORGET( verify_metrics_fn );
  FD_COMPILER_FORGET( dedup_metrics_fn );
  int out_fds[ 2 ];
  ulong nfds = verify_populate_allowed_fds( NULL, NULL, 2UL, out_fds );
  FD_TEST( nfds>=1UL && nfds<=2UL );
  struct sock_filter verify_filter[ 32 ];
  verify_populate_allowed_seccomp( NULL, NULL, 32UL, verify_filter );

  nfds = dedup_populate_allowed_fds( NULL, NULL, 2UL, out_fds );
  FD_TEST( nfds>=1UL && nfds<=2UL );
  struct sock_filter dedup_filter[ 32 ];
  dedup_populate_allowed_seccomp( NULL, NULL, 32UL, dedup_filter );
}

static ulong
write_input_txn( fd_topo_t const * topo,
                 fd_topo_link_t *  link,
                 ulong             chunk,
                 uchar             source_tpu,
                 uint              bam_seq_id ) {
  fd_wksp_t * mem = topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ].wksp;
  fd_txn_m_t * txnm = (fd_txn_m_t *)fd_chunk_to_laddr( mem, chunk );
  *txnm = (fd_txn_m_t) {
    .reference_slot = 0UL,
    .payload_sz     = (ushort)sample_vote_sz,
    .txn_t_sz       = 0U,
    .source_ipv4    = 0U,
    .source_tpu     = source_tpu,
    .scheduler_arrival_tspub = 0U,
    .block_engine   = {0},
    .bam = {
      .max_schedule_slot = 123UL,
      .seq_id            = bam_seq_id,
      .txn_cnt           = 1U,
      .batch_idx         = 0U,
      .revert_on_error   = 0U,
    },
  };
  fd_memcpy( fd_txn_m_payload( txnm ), sample_vote, sample_vote_sz );
  return fd_txn_m_realized_footprint( txnm, 0, 0 );
}

static void
run_duplicate_pipeline( ulong source_tpu,
                        ulong verify_in_idx,
                        ulong expected_verify_published,
                        ulong expected_dedup_published,
                        ulong expected_verify_dedup_fail_cnt,
                        ulong expected_dedup_dedup_fail_cnt ) {
  test_free_all();
  fd_topo_t * topo = mock_topo_create();

  fd_topo_tile_t * verify_tile = &topo->tiles[ fd_topo_find_tile( topo, "verify", 0UL ) ];
  fd_topo_tile_t * dedup_tile  = &topo->tiles[ fd_topo_find_tile( topo, "dedup",  0UL ) ];
  verify_privileged_init( topo, verify_tile );
  verify_unprivileged_init( topo, verify_tile );
  dedup_privileged_init( topo, dedup_tile );
  test_dedup_init( topo, dedup_tile );

  fd_verify_ctx_t * verify_ctx = fd_topo_obj_laddr( topo, verify_tile->tile_obj_id );
  fd_dedup_ctx_t *  dedup_ctx  = fd_topo_obj_laddr( topo, dedup_tile->tile_obj_id );

  fd_topo_link_t * input_link       = &topo->links[ verify_tile->in_link_id[ verify_in_idx ] ];
  fd_topo_link_t * verify_out_link  = &topo->links[ verify_tile->out_link_id[ 0 ] ];
  fd_topo_link_t * dedup_out_link   = &topo->links[ dedup_tile->out_link_id[ 0 ] ];

  test_stem_t verify_stem[ 1 ];
  test_stem_t dedup_stem [ 1 ];
  test_stem_init( verify_stem, verify_out_link->mcache, verify_out_link->depth );
  test_stem_init( dedup_stem,  dedup_out_link->mcache,  dedup_out_link->depth  );

  fd_wksp_t * input_mem = topo->workspaces[ topo->objs[ input_link->dcache_obj_id ].wksp_id ].wksp;
  ulong input_chunk0 = fd_dcache_compact_chunk0( input_mem, input_link->dcache );
  ulong input_wmark  = fd_dcache_compact_wmark ( input_mem, input_link->dcache, input_link->mtu );
  ulong input_chunk  = input_chunk0;

  for( ulong i=0UL; i<2UL; i++ ) {
    ulong input_sz = write_input_txn( topo, input_link, input_chunk, (uchar)source_tpu, 77U );
    FD_TEST( !verify_before_frag( verify_ctx, verify_in_idx, i, 0UL ) );
    verify_during_frag( verify_ctx, verify_in_idx, i, 0UL, input_chunk, input_sz, 0UL );
    verify_after_frag( verify_ctx, verify_in_idx, i, 0UL, input_sz, 0UL, 0UL, &verify_stem->stem );
    input_chunk = fd_dcache_compact_next( input_chunk, input_sz, input_chunk0, input_wmark );
  }

  FD_TEST( verify_stem->seqs[ 0 ] == expected_verify_published );
  FD_TEST( verify_ctx->metrics.dedup_fail_cnt == expected_verify_dedup_fail_cnt );

  for( ulong seq=0UL; seq<verify_stem->seqs[ 0 ]; seq++ ) {
    fd_frag_meta_t const * meta = &verify_stem->mcache[ fd_mcache_line_idx( seq, verify_out_link->depth ) ];
    dedup_during_frag( dedup_ctx, DEDUP_IN_IDX_VERIFY, meta->seq, meta->sig, meta->chunk, meta->sz, meta->ctl );
    dedup_after_frag( dedup_ctx, DEDUP_IN_IDX_VERIFY, meta->seq, meta->sig, meta->sz, meta->tsorig, meta->tspub, &dedup_stem->stem );
  }

  FD_TEST( dedup_stem->seqs[ 0 ] == expected_dedup_published );
  FD_TEST( dedup_ctx->metrics.dedup_fail_cnt == expected_dedup_dedup_fail_cnt );

  for( ulong seq=0UL; seq<dedup_stem->seqs[ 0 ]; seq++ ) {
    fd_frag_meta_t const * meta = &dedup_stem->mcache[ fd_mcache_line_idx( seq, dedup_out_link->depth ) ];
    fd_txn_m_t const * txnm = (fd_txn_m_t const *)fd_chunk_to_laddr_const( dedup_ctx->out_mem, meta->chunk );
    FD_TEST( txnm->source_tpu == (uchar)source_tpu );
    FD_TEST( txnm->payload_sz == sample_vote_sz );
  }

  test_free_all();
}

static void
test_bam_duplicates_survive_prepack_pipeline( void ) {
  FD_LOG_NOTICE(( "test_bam_duplicates_survive_prepack_pipeline" ));
  run_duplicate_pipeline( FD_TXN_M_TPU_SOURCE_BAM,
                          VERIFY_IN_IDX_BAM,
                          2UL,
                          2UL,
                          0UL,
                          0UL );
}

static void
test_quic_duplicates_still_dedup_prepack( void ) {
  FD_LOG_NOTICE(( "test_quic_duplicates_still_dedup_prepack" ));
  run_duplicate_pipeline( FD_TXN_M_TPU_SOURCE_QUIC,
                          VERIFY_IN_IDX_QUIC,
                          1UL,
                          1UL,
                          1UL,
                          0UL );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  test_seccomp();
  test_bam_duplicates_survive_prepack_pipeline();
  test_quic_duplicates_still_dedup_prepack();
  test_free_all();

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
