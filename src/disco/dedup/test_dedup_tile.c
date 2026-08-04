#define FD_TILE_TEST
#include "fd_dedup_tile.c"

static void (* const test_init_refs[])( fd_topo_t const *, fd_topo_tile_t const * ) __attribute__((unused)) = {
  privileged_init,
  unprivileged_init,
};

static ulong (* const test_policy_refs[])( fd_topo_t const *, fd_topo_tile_t const *, ulong, struct sock_filter * ) __attribute__((unused)) = {
  populate_allowed_seccomp,
};

static ulong (* const test_fds_refs[])( fd_topo_t const *, fd_topo_tile_t const *, ulong, int * ) __attribute__((unused)) = {
  populate_allowed_fds,
};

typedef struct {
  fd_dedup_ctx_t ctx;

  uchar          dcache[ 16UL*FD_TPU_PARSED_MTU ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_frag_meta_t mcache[ 16UL ]                    __attribute__((aligned(alignof(fd_frag_meta_t))));

  fd_frag_meta_t * mcaches[ 1 ];
  ulong            seqs[ 1 ];
  ulong            depths[ 1 ];
  ulong            cr_avail[ 1 ];
  ulong            min_cr_avail;
  int              out_reliable[ 1 ];
  fd_stem_context_t stem;
} test_env_t;

static void
test_env_init( test_env_t * env ) {
  fd_memset( env, 0, sizeof(test_env_t) );

  env->ctx.in_kind[ 0 ] = IN_KIND_VERIFY;
  env->ctx.out_mem       = (fd_wksp_t *)env->dcache;
  env->ctx.out_chunk0    = 0UL;
  env->ctx.out_wmark     = sizeof(env->dcache)/FD_CHUNK_SZ-1UL;
  env->ctx.out_chunk     = 0UL;

  env->mcaches[ 0 ]     = env->mcache;
  env->depths[ 0 ]      = 16UL;
  env->cr_avail[ 0 ]    = ULONG_MAX;
  env->min_cr_avail     = ULONG_MAX;
  env->out_reliable[ 0 ] = 0;
  env->stem = (fd_stem_context_t) {
    .mcaches             = env->mcaches,
    .seqs                = env->seqs,
    .depths              = env->depths,
    .cr_avail            = env->cr_avail,
    .min_cr_avail        = &env->min_cr_avail,
    .cr_decrement_amount = 0UL,
    .out_reliable        = env->out_reliable,
  };
}

static void
test_publish( test_env_t * env,
              int          is_bam,
              ulong        bundle_id,
              uint         seq_id,
              uchar        txn_cnt,
              uchar        batch_idx,
              uchar        signature,
              int          preprocess_failed ) {
  fd_txn_m_t * txnm = (fd_txn_m_t *)fd_chunk_to_laddr( env->ctx.out_mem, env->ctx.out_chunk );
  *txnm = (fd_txn_m_t) {
    .payload_sz = FD_TXN_SIGNATURE_SZ,
    .txn_t_sz   = (ushort)fd_txn_footprint( 0UL, 0UL ),
    .source_tpu = (uchar)(is_bam ? FD_TXN_M_TPU_SOURCE_BAM : FD_TXN_M_TPU_SOURCE_BUNDLE),
    .block_engine = {
      .bundle_id      = bundle_id,
      .bundle_txn_cnt = batch_idx ? 0UL : txn_cnt,
    },
    .bam = {
      .seq_id            = seq_id,
      .txn_cnt           = txn_cnt,
      .batch_idx         = batch_idx,
      .revert_on_error   = !!(is_bam && bundle_id),
      .preprocess_failed = !!preprocess_failed,
    },
  };

  fd_memset( fd_txn_m_payload( txnm ), signature, FD_TXN_SIGNATURE_SZ );
  fd_memset( fd_txn_m_txn_t( txnm ), 0, fd_txn_footprint( 0UL, 0UL ) );

  after_frag( &env->ctx, 0UL, 0UL, 0UL, fd_txn_m_realized_footprint( txnm, 1, 0 ), 0UL, 0UL, &env->stem );
}

/* Treating equal BAM and Block Engine IDs as one group suppresses the reset and
   can overflow bundle_idx.  Qualify IDs by source and reset at batch_idx zero. */
static void
test_bam_block_engine_namespace( void ) {
  test_env_t env[ 1 ];

  /* Non-revert BAM work has no block-engine bundle ID but must still be
     pinned to resolver 0, including a later singleton. */
  test_env_init( env );
  test_publish( env, 1, 0UL, 17U, 2U, 0U, 0x30U, 0 );
  FD_TEST( env->mcache[ fd_mcache_line_idx( env->seqs[ 0 ]-1UL, env->depths[ 0 ] ) ].sig==1UL );
  test_publish( env, 1, 0UL, 18U, 1U, 0U, 0x40U, 0 );
  FD_TEST( env->mcache[ fd_mcache_line_idx( env->seqs[ 0 ]-1UL, env->depths[ 0 ] ) ].sig==1UL );

  /* Reusing a raw ID across sources or after a singleton must not carry the
     prior source's bundle index into the next group. */
  struct {
    uint  seq_id;
    uchar bam_cnt;
    uchar be_cnt;
  } const cases[] = {
    {  0U, 5U, 1U },
    { 17U, 1U, 5U },
  };
  for( ulong case_idx=0UL; case_idx<sizeof(cases)/sizeof(cases[0]); case_idx++ ) {
    test_env_init( env );
    uint  seq_id   = cases[ case_idx ].seq_id;
    uchar bam_cnt  = cases[ case_idx ].bam_cnt;
    uchar be_cnt   = cases[ case_idx ].be_cnt;
    ulong bundle_id = (ulong)seq_id+1UL;

    for( uchar i=0U; i<bam_cnt; i++ )
      test_publish( env, 1, bundle_id, seq_id, bam_cnt, i, (uchar)(0x10U+i), 0 );
    for( uchar i=0U; i<be_cnt; i++ )
      test_publish( env, 0, bundle_id, 0U, be_cnt, i, (uchar)(0x20U+i), 0 );

    FD_TEST( env->seqs[ 0 ]==(ulong)bam_cnt+(ulong)be_cnt );
  }

  /* Equal IDs and equal signatures across sources are not duplicates. */
  test_env_init( env );
  test_publish( env, 1, 18UL, 17U, 1U, 0U, 0x42U, 0 );
  test_publish( env, 0, 18UL, 0U, 1U, 0U, 0x42U, 0 );
  FD_TEST( env->seqs[ 0 ]==2UL );

  /* A failed BAM singleton must not suppress the next block-engine bundle. */
  test_env_init( env );
  test_publish( env, 1, 18UL, 17U, 1U, 0U, 0x42U, 1 );
  test_publish( env, 0, 18UL, 0U, 1U, 0U, 0x43U, 0 );
  FD_TEST( env->seqs[ 0 ]==2UL );

  /* Source isolation is symmetric. */
  test_env_init( env );
  test_publish( env, 0, 18UL, 0U, 1U, 0U, 0x42U, 0 );
  test_publish( env, 1, 18UL, 17U, 1U, 0U, 0x42U, 0 );
  FD_TEST( env->seqs[ 0 ]==2UL );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  test_bam_block_engine_namespace();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
