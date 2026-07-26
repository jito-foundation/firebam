/* test_verify_tile.c injects mock inputs into the verify tile.
   Uses libc malloc instead of wksp alloc for better memory sanitization
   check accuracy. */

#define FD_TILE_TEST
#include "fd_verify_tile.c"
#include "../topo/fd_topob.h"
#include "../quic/fd_tpu.h"
#include <stdlib.h>
#include <unistd.h>

#define TCACHE_DEPTH (128UL)

static void
test_seccomp( void ) {
  int   out_fds[2];
  ulong nfds = populate_allowed_fds( NULL, NULL, 2UL, out_fds );
  FD_TEST( nfds>=1 && nfds<=2 );
  FD_TEST( out_fds[0]==STDERR_FILENO );
  if( nfds==2 ) FD_TEST( out_fds[1]==fd_log_private_logfile_fd() );

  struct sock_filter filter[ 32 ];
  populate_allowed_seccomp( NULL, NULL, 32UL, filter );
}

#define TEST_ALLOC_MAX (64UL)
static void * test_allocs[ TEST_ALLOC_MAX ];
static ulong  test_alloc_cnt;

static void *
test_malloc( ulong align, ulong sz ) {
  FD_TEST( test_alloc_cnt<TEST_ALLOC_MAX );
  void * p = aligned_alloc( align, sz );
  FD_TEST( p );
  test_allocs[ test_alloc_cnt ] = p;
  test_alloc_cnt++;
  return p;
}

static void
test_free_all( void ) {
  while( test_alloc_cnt ) free( test_allocs[ --test_alloc_cnt ] );
}

static void
mock_link_create( fd_topo_t *  topo,
                  char const * name ) {
#define LINK_DEPTH (16UL)
  fd_topo_link_t * link = fd_topob_link( topo, name, "wksp", LINK_DEPTH, 0UL, 0UL );
  ulong data_sz = fd_dcache_req_data_sz( FD_TPU_REASM_MTU, LINK_DEPTH, 1UL, 1 );
  link->mcache  = fd_mcache_join( fd_mcache_new( test_malloc( fd_mcache_align(), fd_mcache_footprint( LINK_DEPTH, 0UL ) ), LINK_DEPTH, 0UL, 0UL ) );
  link->dcache  = fd_dcache_join( fd_dcache_new( test_malloc( fd_dcache_align(), fd_dcache_footprint( data_sz, 0UL ) ), data_sz, 0UL ) );
}

static fd_topo_t *
mock_topo_create( void ) {
  fd_topo_t * topo = fd_topob_new( test_malloc( alignof(fd_topo_t), sizeof(fd_topo_t) ), "verify-test" );

  fd_topo_wksp_t * wksp = fd_topob_wksp( topo, "wksp" );
  wksp->wksp = NULL;

  fd_topo_tile_t * verify = fd_topob_tile( topo, "verify", "wksp", "wksp", 0UL, 0, 0, 0 );
  verify->verify.tcache_depth = TCACHE_DEPTH;

  fd_verify_ctx_t * ctx = test_malloc( scratch_align(), scratch_footprint( verify ) );
  topo->objs[ verify->tile_obj_id ].offset = (ulong)ctx;

  mock_link_create( topo, "quic_verify"  );
  mock_link_create( topo, "bundle_verif" );
  mock_link_create( topo, "bam_verif"    );
  mock_link_create( topo, "gossip_out"   );
  mock_link_create( topo, "txsend_out"   );

  /* Declare link ins in opposite order than IN_KIND_* to check for in
     idx confusion */
#define IN_IDX_TXSEND 0
#define IN_IDX_GOSSIP 1
#define IN_IDX_BAM    2
#define IN_IDX_BUNDLE 3
#define IN_IDX_QUIC   4
  fd_topob_tile_in( topo, "verify", 0UL, "wksp", "txsend_out",   0UL, 0, 1 );
  fd_topob_tile_in( topo, "verify", 0UL, "wksp", "gossip_out",   0UL, 0, 1 );
  fd_topob_tile_in( topo, "verify", 0UL, "wksp", "bam_verif",    0UL, 0, 1 );
  fd_topob_tile_in( topo, "verify", 0UL, "wksp", "bundle_verif", 0UL, 0, 1 );
  fd_topob_tile_in( topo, "verify", 0UL, "wksp", "quic_verify",  0UL, 0, 1 );

  return topo;
}

static void
test_load_balance( void ) {
  test_free_all();
  fd_topo_t *       topo = mock_topo_create();
  fd_topo_tile_t *  tile = &topo->tiles[ fd_topo_find_tile( topo, "verify", 0UL ) ];
  privileged_init( topo, tile );
  unprivileged_init( topo, tile );
  fd_verify_ctx_t * ctx  = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  /* Tile should accept all traffic if it's the only tile */
  ctx->round_robin_idx = 0UL;
  ctx->round_robin_cnt = 1UL;
  FD_TEST( before_frag( ctx, IN_IDX_BUNDLE, 0UL, 0UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_BUNDLE, 1UL, 0UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_BUNDLE, 0UL, 1UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_BUNDLE, 1UL, 1UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_BAM,    0UL, 0UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_BAM,    1UL, 0UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_BAM,    0UL, 1UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_BAM,    1UL, 1UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_QUIC,   0UL, 0UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_QUIC,   1UL, 0UL )==0 );

  /* Tile 0 should accept all bundle traffic */
  ctx->round_robin_idx = 0UL;
  ctx->round_robin_cnt = 4UL;
  FD_TEST( before_frag( ctx, IN_IDX_BUNDLE, 0UL, 1UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_BUNDLE, 1UL, 1UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_BAM,    0UL, 1UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_BAM,    1UL, 1UL )==0 );

  /* Tile 0 should load balance other traffic */
  FD_TEST( before_frag( ctx, IN_IDX_BUNDLE, 0UL, 0UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_BUNDLE, 1UL, 0UL )==1 );
  FD_TEST( before_frag( ctx, IN_IDX_BUNDLE, 2UL, 0UL )==1 );
  FD_TEST( before_frag( ctx, IN_IDX_BAM,    0UL, 0UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_BAM,    1UL, 0UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_BAM,    2UL, 0UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_QUIC,   0UL, 0UL )==0 );
  FD_TEST( before_frag( ctx, IN_IDX_QUIC,   1UL, 0UL )==1 );
  FD_TEST( before_frag( ctx, IN_IDX_QUIC,   2UL, 0UL )==1 );
}

static void
test_bam_atomic_verify_failure_result_owner( void ) {
  fd_verify_ctx_t ctx[1];
  uchar           verify_dcache[ FD_TPU_REASM_MTU ] __attribute__((aligned(FD_CHUNK_ALIGN)));
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

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  test_seccomp();
  test_load_balance();
  test_bam_atomic_verify_failure_result_owner();
  test_free_all();
  /* further tests here ... */

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
