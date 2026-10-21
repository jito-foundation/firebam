/* test_bam_replay_tile checks the replay tile's BAM slot timing: the
   PoH slot duration follows the BAM runtime mode, latched once per
   Tower reset, and Alpenglow leader timing ignores it.  It reuses the
   upstream replay tile harness (mocks, setup_ctx, drive_become_leader,
   test_stem) without running its tests. */

#define main test_replay_tile_main
#include "../../discof/replay/test_replay_tile.c"
#undef main

static void
test_poh_slot_timing_mode( fd_wksp_t * wksp ) {
  static uchar metrics_scratch[ FD_METRICS_FOOTPRINT( 0UL ) ] __attribute__((aligned(FD_METRICS_ALIGN)));
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );

  static fd_replay_tile_t selector_ctx[1];
  fd_memset( selector_ctx, 0, sizeof(selector_ctx) );

  selector_ctx->use_nominal_slot_duration = 0;
  FD_TEST( replay_poh_slot_duration_ns( selector_ctx, &FD_SLOT_PARAMS_400MS )==FD_SLOT_PARAMS_400MS.ns_per_slot_adjusted );
  FD_TEST( replay_poh_slot_duration_ns( selector_ctx, &FD_SLOT_PARAMS_350MS )==FD_SLOT_PARAMS_350MS.ns_per_slot_adjusted );

  selector_ctx->use_nominal_slot_duration = 1;
  FD_TEST( replay_poh_slot_duration_ns( selector_ctx, &FD_SLOT_PARAMS_400MS )==FD_SLOT_PARAMS_400MS.ns_per_slot );
  FD_TEST( replay_poh_slot_duration_ns( selector_ctx, &FD_SLOT_PARAMS_350MS )==FD_SLOT_PARAMS_350MS.ns_per_slot );

  /* Compose the selector with the same future-slot lookup used by the
     leader grace-period path.  The 350 ms feature activates at the
     next epoch boundary (slot 128). */
  static fd_replay_tile_t ctx[1];
  setup_ctx( ctx, wksp );
  fd_bank_t * root_bank = fd_banks_root( ctx->banks );
  FD_TEST( root_bank );
  fd_memset( &root_bank->f.features, 0xFF, sizeof(root_bank->f.features) );
  root_bank->f.features.reduce_slot_time_to_350ms = 1UL;

  fd_slot_params_t future_params = fd_slot_params_at_slot( root_bank, 128UL );
  FD_TEST( future_params.ns_per_slot==FD_SLOT_PARAMS_350MS.ns_per_slot );
  ctx->use_nominal_slot_duration = 0;
  FD_TEST( replay_poh_slot_duration_ns( ctx, &future_params )==FD_SLOT_PARAMS_350MS.ns_per_slot_adjusted );
  ctx->use_nominal_slot_duration = 1;
  FD_TEST( replay_poh_slot_duration_ns( ctx, &future_params )==FD_SLOT_PARAMS_350MS.ns_per_slot );

  /* Runtime changes are latched once when replay accepts a reset, so
     the reset message and all subsequent leader timing agree. */
  fd_wksp_reset( wksp, 42U );
  setup_ctx( ctx, wksp );
  fd_hash_t runtime_root_id = { .ul = { 8999UL } };
  init_root_fec( ctx, &runtime_root_id );
  fd_bank_t * runtime_root_bank = fd_banks_root( ctx->banks );
  FD_TEST( runtime_root_bank );

  fd_bam_ctrl_t bam_ctrl = { .applied_enable = 0U };
  ctx->bam_ctrl                  = &bam_ctrl;
  ctx->use_nominal_slot_duration = 1;
  fd_tower_slot_done_t tower_reset = {
    .replay_slot     = runtime_root_bank->f.slot,
    .replay_bank_idx = runtime_root_bank->idx,
    .vote_slot       = ULONG_MAX,
    .reset_slot      = runtime_root_bank->f.slot,
    .reset_block_id  = runtime_root_id,
    .root_slot       = ULONG_MAX,
  };
  for( uchar enable=0U; enable<=1U; enable++ ) {
    bam_ctrl.applied_enable   = enable;
    ctx->reset_cmr.ul[ 0 ]    = 8998UL-enable; /* make each tower reset a new block on main */
    runtime_root_bank->refcnt = 1UL;
    ulong reset_chunk = ctx->replay_out->chunk;
    process_tower_slot_done( ctx, test_stem, &tower_reset, (ulong)enable );
    fd_poh_reset_t const * runtime_reset = fd_chunk_to_laddr_const( ctx->replay_out->mem, reset_chunk );
    ulong slot_ns = enable ? FD_SLOT_PARAMS_400MS.ns_per_slot : FD_SLOT_PARAMS_400MS.ns_per_slot_adjusted;
    FD_TEST( ctx->use_nominal_slot_duration==enable );
    FD_TEST( runtime_reset->tick_duration_ns==slot_ns/runtime_reset->ticks_per_slot );
    FD_TEST( ctx->metrics.slot_duration_ns==slot_ns );
  }

  /* Verify the effective duration in leader messages for both startup
     modes.  The Tower reset messages above cover both runtime modes. */
  for( int use_nominal=0; use_nominal<=1; use_nominal++ ) {
    fd_wksp_reset( wksp, 42U );
    setup_ctx( ctx, wksp );
    ctx->use_nominal_slot_duration = use_nominal;

    fd_hash_t root_id = { .ul = { 9000UL+(ulong)use_nominal } };
    init_root_fec( ctx, &root_id );
    /* Make the reset bank's grace-period lookup at the leader slot
       differ from the leader bank's own (cloned) params.  The leader
       message must follow the leader bank, as upstream does. */
    fd_banks_root( ctx->banks )->f.slot_params_default = FD_SLOT_PARAMS_350MS;

    ulong leader_chunk = ctx->replay_out->chunk;
    fd_bank_t * leader_bank = drive_become_leader( ctx, &root_id, 1UL );
    fd_became_leader_t const * leader = fd_chunk_to_laddr_const( ctx->replay_out->mem, leader_chunk );
    FD_TEST( fd_slot_params_at_slot( fd_banks_root( ctx->banks ), 1UL ).ns_per_slot!=leader_bank->f.slot_params.ns_per_slot );
    FD_TEST( ctx->metrics.slot_duration_ns==(ulong)(leader->slot_end_ns-leader->slot_start_ns) );
    ulong expected_slot_duration_ns = replay_poh_slot_duration_ns( ctx, &leader_bank->f.slot_params );
    FD_TEST( (ulong)(leader->slot_end_ns-leader->slot_start_ns)==expected_slot_duration_ns );
    FD_TEST( leader->tick_duration_ns==expected_slot_duration_ns/leader->ticks_per_slot );

  }

  FD_LOG_NOTICE(( "pass: test_poh_slot_timing_mode" ));
}

/* Under Alpenglow, BAM does not change leader timing: in either mode a
   block ends at its ParentReady deadline less the production buffer and
   broadcast reserve, as Agave's block_timeout and the BAM node's pacer
   assume, with the adjusted tick duration. */

static void
test_ag_bam_slot_timing( fd_wksp_t * wksp ) {
  static fd_replay_tile_t ctx[1];
  for( uint enable=0U; enable<=1U; enable++ ) {
    fd_wksp_reset( wksp, 42U );
    setup_ctx( ctx, wksp );
    fd_hash_t parent_dmr = { .ul = { 0xBA0000UL+enable } };
    setup_ag_block_id_map( ctx, wksp, &parent_dmr );
    setup_votor_input( ctx, wksp );
    fd_bam_ctrl_t bam_ctrl = { .applied_enable = (uchar)enable };
    ctx->alpenglow                 = 1;
    ctx->bam_ctrl                  = &bam_ctrl;
    ctx->use_nominal_slot_duration = (int)enable;
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

    /* The gauge follows the epoch info, not the Tower-only cache. */
    ctx->metrics.slot_duration_ns = 1UL;
    ctx->ns_per_slot              = params->ns_per_slot; /* as refresh_epoch_info */
    replay_bam_metrics_write( ctx );
    FD_TEST( FD_MGAUGE_GET( REPLAY, SLOT_DURATION_NANOS )==params->ns_per_slot_adjusted );
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

  test_poh_slot_timing_mode( wksp );            fd_wksp_reset( wksp, 42U );
  test_ag_bam_slot_timing( wksp );

  FD_TEST( mock_store_view_success_cnt==mock_store_view_release_cnt );

  fd_wksp_delete_anonymous( wksp );
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
