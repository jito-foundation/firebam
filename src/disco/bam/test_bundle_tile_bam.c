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
   are derived from it.  The tile starts with BAM inactive and one Block
   Engine transaction pending. */
static fd_bundle_tile_t *
test_bam_bundle_env_create( test_bundle_env_t * env,
                            fd_wksp_t *         wksp,
                            ulong *             bam_status_fseq,
                            ulong               bundle_seq ) {
  test_bundle_env_create( env, wksp );
  fd_bundle_tile_t * state = env->state;
  fd_clock_tile_init( state->clock );
  FD_TEST( fd_rng_new( state->rng, 0U, 0UL ) );
  FD_TEST( fd_keepalive_init( state->keepalive, state->rng, (long)1e9, (long)1e9, fd_bundle_now( state ) ) );
  state->bam_status_fseq = bam_status_fseq;
  pending_txn_push_tail( state->pending_txns, (fd_bundle_pending_txn_t){ .sig=1UL, .bundle_seq=bundle_seq } );
  return state;
}

/* While BAM controls the TPU, the bundle tile sleeps as it does without
   upcoming leader slots, closing the Block Engine connection, whatever
   housekeeping decides.  Activation also discards the queued Block
   Engine work. */
static void
test_bam_override_sync_clears_pending( fd_wksp_t * wksp ) {
  FD_LOG_NOTICE(( "TEST BAM override sync clears pending transactions" ));

  uchar fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  void * fseq_shmem = fd_fseq_new( fseq_mem, 0UL );
  FD_TEST( fseq_shmem );
  ulong * bam_status_fseq = fd_fseq_join( fseq_shmem );
  FD_TEST( bam_status_fseq );

  /* before_credit must observe an activation that occurred since the
     last housekeeping pass, even with work queued. */
  test_bundle_env_t env[1];
  fd_bundle_tile_t * ctx = test_bam_bundle_env_create( env, wksp, bam_status_fseq, 1UL );
  test_bundle_env_mock_conn( env );

  fd_fseq_update( bam_status_fseq, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  int opt_poll_in = 1;
  int charge_busy = 0;
  before_credit( ctx, env->stem, &charge_busy );

  FD_TEST( env->stem_seqs[ 0 ]==0UL );
  FD_TEST( pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( ctx->bam_override_active );
  FD_TEST( ctx->tcp_sock==-1 );
  FD_TEST( !ctx->tcp_sock_connected );
  FD_TEST( ctx->sleep_mode && ctx->sleep_check_ns==LONG_MAX && ctx->backoff_until==0L );
  FD_TEST( next_deadline( ctx )==LONG_MAX );

  /* The same boundary handles deactivation without discarding pending work. */
  ctx->next_step_deadline = LONG_MAX;
  pending_txn_push_tail( ctx->pending_txns, (fd_bundle_pending_txn_t){ .sig=1UL, .bundle_seq=2UL } );
  long before_resume = fd_log_wallclock();
  fd_fseq_update( bam_status_fseq, 0UL );
  before_credit( ctx, env->stem, &charge_busy );
  FD_TEST( !ctx->bam_override_active );
  FD_TEST( !ctx->sleep_mode && !ctx->sleep_check_ns && !ctx->next_step_deadline );
  FD_TEST( pending_txn_cnt( ctx->pending_txns )==1UL );
  FD_TEST( ctx->last_bundle_status_log_nanos>=before_resume );

  opt_poll_in = 1;
  charge_busy = 0;
  after_credit( ctx, env->stem, &opt_poll_in, &charge_busy );
  FD_TEST( env->stem_seqs[ 0 ]==1UL );
  FD_TEST( pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( !opt_poll_in );
  FD_TEST( charge_busy );
  test_bundle_env_destroy( env );

  /* With replay, housekeeping wakes the tile for a near leader slot when
     an identity switch unhalts it, and deactivation hands the tile back
     to that leader schedule check.  replay_in.mem is only tested for
     presence. */
  void * keyswitch_mem = fd_wksp_alloc_laddr( wksp, fd_keyswitch_align(), fd_keyswitch_footprint(), 1UL );
  FD_TEST( keyswitch_mem );
  fd_keyswitch_t * keyswitch = fd_keyswitch_join( fd_keyswitch_new( keyswitch_mem, FD_KEYSWITCH_STATE_UNLOCKED ) );
  FD_TEST( keyswitch );
  ctx = test_bam_bundle_env_create( env, wksp, bam_status_fseq, 3UL );
  ctx->keyswitch        = keyswitch;
  ctx->replay_in.mem    = wksp;
  ctx->reset_slot       = 100UL;
  ctx->next_leader_slot = 110UL;

  fd_fseq_update( bam_status_fseq, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  before_credit( ctx, env->stem, &charge_busy );
  FD_TEST( ctx->bam_override_active && ctx->sleep_mode && ctx->sleep_check_ns==LONG_MAX );

  /* UNHALT while BAM is active: housekeeping wakes the tile, and the
     next before_credit puts it back to sleep without dialing. */
  ctx->halt_signing = 1;
  fd_keyswitch_state( keyswitch, FD_KEYSWITCH_STATE_UNHALT_PENDING );
  fd_bundle_tile_housekeeping( ctx );
  FD_TEST( !ctx->halt_signing && !ctx->sleep_mode );
  before_credit( ctx, env->stem, &charge_busy );
  FD_TEST( ctx->sleep_mode && ctx->sleep_check_ns==LONG_MAX );
  FD_TEST( ctx->tcp_sock==-1 && next_deadline( ctx )==LONG_MAX );
  fd_bundle_tile_housekeeping( ctx );
  FD_TEST( ctx->sleep_mode );

  /* Deactivation with replay stays asleep until housekeeping rechecks
     the leader schedule: a far leader keeps it asleep, a near one
     wakes it. */
  ctx->next_leader_slot = 1000UL;
  fd_fseq_update( bam_status_fseq, 0UL );
  before_credit( ctx, env->stem, &charge_busy );
  FD_TEST( !ctx->bam_override_active && ctx->sleep_mode && !ctx->sleep_check_ns && !ctx->next_step_deadline );
  FD_TEST( ctx->tcp_sock==-1 );
  fd_bundle_tile_housekeeping( ctx );
  FD_TEST( ctx->sleep_mode && ctx->sleep_check_ns>0L );
  ctx->sleep_check_ns   = 0L;
  ctx->next_leader_slot = 110UL;
  fd_bundle_tile_housekeeping( ctx );
  FD_TEST( !ctx->sleep_mode && !ctx->next_step_deadline );
  test_bundle_env_destroy( env );
  fd_wksp_free_laddr( fd_keyswitch_delete( fd_keyswitch_leave( keyswitch ) ) );

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
