/* FireBAM bundle tile tests.  test_bundle_tile.c is included verbatim so
   these tests share its tile build; its main is renamed and not run
   here. */

#define main test_bundle_tile_upstream_main
int test_bundle_tile_upstream_main( int argc, char ** argv );
#include "../bundle/test_bundle_tile.c"
#undef main

#include "../bundle/test_bundle_common.c"

/* after_credit recalibrates the tile clock, which upstream's bundle env
   leaves uninitialized.  Initialize it before the keepalive deadlines
   are derived from it. */
static test_bundle_env_t *
test_bam_bundle_env_create( test_bundle_env_t * env,
                            fd_wksp_t *         wksp ) {
  test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;
  fd_clock_tile_init( state->clock );
  FD_TEST( fd_rng_new( state->rng, 0U, 0UL ) );
  FD_TEST( fd_keepalive_init( state->keepalive, state->rng, (long)1e9, (long)1e9, fd_bundle_now( state ) ) );
  return env;
}

/* A stale ownership snapshot can publish queued Block Engine work after BAM
   takes exclusive TPU control.  Recheck the override and discard that queue. */
static void
test_bam_override_sync_clears_pending( fd_wksp_t * wksp ) {
  FD_LOG_NOTICE(( "TEST BAM override sync clears pending transactions" ));

  uchar fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  void * fseq_shmem = fd_fseq_new( fseq_mem, 0UL );
  FD_TEST( fseq_shmem );
  ulong * bam_status_fseq = fd_fseq_join( fseq_shmem );
  FD_TEST( bam_status_fseq );

  /* after_credit must observe an activation that occurred since the last
     housekeeping pass and discard queued Block Engine work. */
  test_bundle_env_t env[1];
  test_bam_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );
  fd_bundle_tile_t * ctx = env->state;
  ctx->bam_status_fseq    = bam_status_fseq;
  ctx->bam_override_active = 0;
  pending_txn_push_tail( ctx->pending_txns, (fd_bundle_pending_txn_t){ .sig=1UL, .bundle_seq=1UL } );

  long before_pause = fd_log_wallclock();
  fd_fseq_update( bam_status_fseq, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  int opt_poll_in = 1;
  int charge_busy = 0;
  after_credit( ctx, env->stem, &opt_poll_in, &charge_busy );

  FD_TEST( env->stem_seqs[ 0 ]==0UL );
  FD_TEST( pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( ctx->bam_override_active );
  FD_TEST( ctx->tcp_sock==-1 );
  FD_TEST( !ctx->tcp_sock_connected );
  FD_TEST( ctx->bundle_status_plugin==127 );
  FD_TEST( ctx->bundle_status_recent==FD_BUNDLE_STATE_DISCONNECTED );
  FD_TEST( ctx->bundle_status_logged==FD_BUNDLE_STATE_DISCONNECTED );
  FD_TEST( ctx->last_bundle_status_log_nanos>=before_pause );

  /* The same boundary handles deactivation without discarding pending work. */
  ctx->backoff_until = fd_log_wallclock() + (long)30e9;
  ctx->defer_reset   = 1;
  pending_txn_push_tail( ctx->pending_txns, (fd_bundle_pending_txn_t){ .sig=1UL, .bundle_seq=2UL } );
  long before_resume = fd_log_wallclock();
  fd_fseq_update( bam_status_fseq, 0UL );
  before_credit( ctx, env->stem, &charge_busy );
  FD_TEST( !ctx->bam_override_active );
  FD_TEST( ctx->backoff_until==0L );
  FD_TEST( !ctx->defer_reset );
  FD_TEST( pending_txn_cnt( ctx->pending_txns )==1UL );
  FD_TEST( ctx->last_bundle_status_log_nanos>=before_resume );

  /* An uncontended publication claims and releases the shared status word. */
  opt_poll_in = 1;
  charge_busy = 0;
  after_credit( ctx, env->stem, &opt_poll_in, &charge_busy );
  FD_TEST( env->stem_seqs[ 0 ]==1UL );
  FD_TEST( pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( fd_fseq_query( bam_status_fseq )==0UL );
  FD_TEST( !opt_poll_in );
  FD_TEST( charge_busy );
  test_bundle_env_destroy( env );

  /* before_credit must perform the same transition even when the pending
     queue is nonempty and the client step would otherwise be skipped. */
  test_bam_bundle_env_create( env, wksp );
  ctx = env->state;
  ctx->bam_status_fseq     = bam_status_fseq;
  ctx->bam_override_active = 0;
  pending_txn_push_tail( ctx->pending_txns, (fd_bundle_pending_txn_t){ .sig=1UL, .bundle_seq=2UL } );

  fd_fseq_update( bam_status_fseq, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  charge_busy = 0;
  before_credit( ctx, env->stem, &charge_busy );

  FD_TEST( pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( ctx->bam_override_active );
  test_bundle_env_destroy( env );

  /* A publication claim held by another bundle producer prevents this tile
     from publishing or modifying the queue. */
  test_bam_bundle_env_create( env, wksp );
  ctx = env->state;
  ctx->bam_status_fseq     = bam_status_fseq;
  ctx->bam_override_active = 0;
  pending_txn_push_tail( ctx->pending_txns, (fd_bundle_pending_txn_t){ .sig=1UL, .bundle_seq=3UL } );

  fd_fseq_update( bam_status_fseq, FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING );
  opt_poll_in = 1;
  charge_busy = 0;
  after_credit( ctx, env->stem, &opt_poll_in, &charge_busy );

  FD_TEST( env->stem_seqs[ 0 ]==0UL );
  FD_TEST( pending_txn_cnt( ctx->pending_txns )==1UL );
  FD_TEST( !ctx->bam_override_active );
  FD_TEST( fd_fseq_query( bam_status_fseq )==FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING );
  FD_TEST( opt_poll_in );
  FD_TEST( !charge_busy );

  fd_fseq_update( bam_status_fseq, 0UL );
  after_credit( ctx, env->stem, &opt_poll_in, &charge_busy );
  FD_TEST( env->stem_seqs[ 0 ]==1UL );
  FD_TEST( pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( fd_fseq_query( bam_status_fseq )==0UL );
  test_bundle_env_destroy( env );

  FD_TEST( fd_fseq_leave( bam_status_fseq )==fseq_shmem );
  FD_TEST( fd_fseq_delete( fseq_shmem )==fseq_shmem );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  ulong cpu_idx = fd_tile_cpu_id( fd_tile_idx() );
  if( cpu_idx>fd_shmem_cpu_cnt() ) cpu_idx = 0UL;
  /* Transaction V1 raises FD_TPU_PARSED_MTU enough that the 128-entry test
     dcache and pending deque no longer fit in a 1 MiB workspace. */
  fd_wksp_t * wksp = fd_wksp_new_anonymous( FD_SHMEM_NORMAL_PAGE_SZ, 512UL, fd_shmem_cpu_idx( fd_shmem_numa_idx( cpu_idx ) ), "wksp", 16UL );
  FD_TEST( wksp );

  test_bam_override_sync_clears_pending( wksp );

  fd_wksp_usage_t wksp_usage;
  FD_TEST( fd_wksp_usage( wksp, NULL, 0UL, &wksp_usage ) );
  FD_TEST( wksp_usage.free_cnt==wksp_usage.total_cnt );
  fd_wksp_delete_anonymous( wksp );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
