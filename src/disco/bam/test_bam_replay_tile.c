/* BAM preserves upstream Alpenglow leader deadlines with BAM enabled
   or disabled in the topology and reports the effective slot duration.  Reuse the upstream
   replay harness without running its tests. */

#define main test_replay_tile_main
#include "../../discof/replay/test_replay_tile.c"
#undef main

/* Under Alpenglow, BAM does not change leader timing: with or without BAM a
   block ends at its ParentReady deadline less the production buffer and
   broadcast reserve, as Agave's block_timeout and the BAM node's pacer
   assume, with the adjusted tick duration. */

static void
test_ag_bam_slot_timing( fd_wksp_t * wksp ) {
  static uchar metrics_scratch[ FD_METRICS_FOOTPRINT( 0UL ) ] __attribute__((aligned(FD_METRICS_ALIGN)));
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );
  static fd_replay_tile_t ctx[1];
  for( uint enable=0U; enable<=1U; enable++ ) {
    fd_wksp_reset( wksp, 42U );
    setup_ctx( ctx, wksp );
    fd_hash_t parent_dmr = { .ul = { 0xBA0000UL+enable } };
    setup_ag_root( ctx, &parent_dmr );
    setup_votor_input( ctx, wksp );
    ctx->bam_enabled              = (int)enable;
    mock_leader_for_slot_override  = 1; /* we lead the slot below */
    mock_leader_schedule_loaded    = 1;
    mock_slot_leader               = ctx->identity_pubkey[ 0 ];

    long const window_start = 10000000000L; /* votor's ParentReady time */
    fd_clock_tile_set( ctx->clock, window_start+150000000L );
    ctx->next_leader_slot = AG_SLOTS_PER_WINDOW;
    *ctx->votor_leader = (fd_votor_leader_t){ .slot = AG_SLOTS_PER_WINDOW, .parent_slot = 0UL, .parent_block_id = parent_dmr, .parent_ready_ns = window_start };
    ulong out_idx = ctx->replay_out->idx;
    ulong seq     = test_stem_seqs[ out_idx ];
    FD_TEST( try_become_leader_ag( ctx, test_stem, FD_METRICS_SAMPLE_REASON_LEADER_START, 0UL ) );
    fd_frag_meta_t const * meta = test_stem_mcaches[ out_idx ] + fd_mcache_line_idx( seq+1UL, test_stem_depths[ out_idx ] );
    FD_TEST( meta->sig==REPLAY_SIG_BECAME_LEADER );
    fd_became_leader_t const * leader = fd_chunk_to_laddr_const( ctx->replay_out->mem, meta->chunk );
    fd_slot_params_t const *   params = &ctx->leader_bank->f.slot_params;
    FD_TEST( leader->slot_end_ns==window_start+(long)params->ns_per_slot-(long)FD_TARGET_SLOT_ADJUSTMENT_NS-AG_TIME_TO_COMPLETE_BROADCAST_NS );
    FD_TEST( leader->tick_duration_ns==params->ns_per_slot_adjusted/leader->ticks_per_slot );

    /* The gauge follows the current epoch info. */
    ctx->ns_per_slot      = params->ns_per_slot; /* as refresh_epoch_info */
    FD_MGAUGE_SET( REPLAY, SLOT_DURATION_NANOS, 0UL );
    metrics_write( ctx ); /* reaches replay_bam_metrics_write through its hook */
    FD_TEST( FD_MGAUGE_GET( REPLAY, SLOT_DURATION_NANOS )==(enable ? params->ns_per_slot_adjusted : 0UL) );
  }
  FD_LOG_NOTICE(( "pass: test_ag_bam_slot_timing" ));
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  char const * _page_sz = fd_env_strip_cmdline_cstr ( &argc, &argv, "--page-sz",  NULL, "normal"                 );
  ulong        page_cnt = fd_env_strip_cmdline_ulong( &argc, &argv, "--page-cnt", NULL, 65536UL                  );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( &argc, &argv, "--numa-idx", NULL, fd_shmem_numa_idx( 0UL ) );
  fd_wksp_t * wksp      = fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ), page_cnt, fd_shmem_cpu_idx( numa_idx ), "wksp", 0UL );
  FD_TEST( wksp );

  test_ag_bam_slot_timing( wksp );

  FD_TEST( mock_store_view_success_cnt==mock_store_view_release_cnt );

  fd_wksp_delete_anonymous( wksp );
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
