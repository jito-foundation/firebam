/* test_bam_replay_tile checks the replay tile's BAM slot timing: the
   PoH slot duration follows the BAM runtime mode, latched once per
   Tower reset or Alpenglow leader slot.  It reuses the upstream replay
   tile harness (mocks, setup_ctx, drive_become_leader, test_stem)
   without running its tests. */

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
  setup_ctx( ctx, wksp );
  fd_hash_t runtime_root_id = { .ul = { 8999UL } };
  init_root_fec( ctx, &runtime_root_id );
  fd_bank_t * runtime_root_bank = fd_banks_root( ctx->banks );
  FD_TEST( runtime_root_bank );

  fd_bam_ctrl_t bam_ctrl = { .applied_enable = 0U };
  ctx->bam_ctrl                     = &bam_ctrl;
  ctx->use_nominal_slot_duration    = 1;
  ctx->reset_cmr.ul[ 0 ]             = 8998UL; /* make the tower reset a new block on main */
  fd_tower_slot_done_t tower_reset = {
    .replay_slot     = runtime_root_bank->f.slot,
    .replay_bank_idx = runtime_root_bank->idx,
    .vote_slot       = ULONG_MAX,
    .reset_slot      = runtime_root_bank->f.slot,
    .reset_block_id  = runtime_root_id,
    .root_slot       = ULONG_MAX,
  };

  runtime_root_bank->refcnt = 1UL;
  ulong reset_chunk = ctx->replay_out->chunk;
  process_tower_slot_done( ctx, test_stem, &tower_reset, 0UL );
  fd_poh_reset_t const * runtime_reset = fd_chunk_to_laddr_const( ctx->replay_out->mem, reset_chunk );
  FD_TEST( !ctx->use_nominal_slot_duration );
  FD_TEST( runtime_reset->tick_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot_adjusted/runtime_reset->ticks_per_slot );
  FD_TEST( ctx->metrics.slot_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot_adjusted );

  bam_ctrl.applied_enable   = 1U;
  ctx->reset_cmr.ul[ 0 ] = 8997UL; /* exercise another accepted reset, not a duplicate */
  runtime_root_bank->refcnt = 1UL;
  reset_chunk = ctx->replay_out->chunk;
  process_tower_slot_done( ctx, test_stem, &tower_reset, 1UL );
  runtime_reset = fd_chunk_to_laddr_const( ctx->replay_out->mem, reset_chunk );
  FD_TEST( ctx->use_nominal_slot_duration );
  FD_TEST( runtime_reset->tick_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot/runtime_reset->ticks_per_slot );
  FD_TEST( ctx->metrics.slot_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot );

  /* Verify the effective duration in leader messages for both startup
     modes.  The Tower reset messages above cover both runtime modes. */
  for( int use_nominal=0; use_nominal<=1; use_nominal++ ) {
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

/* Under Alpenglow there is no Tower reset, so replay latches the BAM
   runtime timing mode when it commits to a leader slot.  The RESET and
   BECAME_LEADER of that slot must agree, and a toggle while leader only
   takes effect at the next leader slot.  Pack's deadline follows the
   original ParentReady window even if parent replay or the previous
   block delays entry, with one production adjustment and a broadcast
   reserve.  Tick duration is independent of that remaining deadline. */

static void
test_ag_bam_runtime_slot_timing_mode( fd_wksp_t * wksp ) {
  static fd_replay_tile_t ctx[1];
  for( int startup_nominal=0; startup_nominal<=1; startup_nominal++ ) {
    fd_wksp_reset( wksp, 42U );
    setup_ctx( ctx, wksp );
    fd_hash_t parent_dmr = { .ul = { 0xBA0000UL+(ulong)startup_nominal } };
    setup_ag_block_id_map( ctx, wksp, &parent_dmr );
    setup_votor_input( ctx, wksp );
    fd_bam_ctrl_t bam_ctrl = { .applied_enable = (uchar)!startup_nominal };
    ctx->alpenglow                 = 1;
    ctx->bam_ctrl                  = &bam_ctrl;
    ctx->use_nominal_slot_duration = startup_nominal;
    mock_leader_for_slot_override  = 1; /* we lead the slots below */
    mock_leader_schedule_loaded    = 1;
    mock_slot_leader               = ctx->identity_pubkey[ 0 ];
    fd_bank_t * parent_bank = fd_banks_root( ctx->banks );
    FD_TEST( parent_bank->f.slot_params.ns_per_slot!=parent_bank->f.slot_params.ns_per_slot_adjusted );

    ulong out_idx = ctx->replay_out->idx;
    ulong in_chunk = ctx->in[ TEST_VOTOR_IN_IDX ].chunk0;
    fd_votor_leader_t * notification = fd_chunk_to_laddr( ctx->in[ TEST_VOTOR_IN_IDX ].mem, in_chunk );
    *notification = (fd_votor_leader_t){ .slot = AG_SLOTS_PER_WINDOW, .parent_slot = 0UL, .parent_block_id = parent_dmr };
    fd_clock_tile_set( ctx->clock, 10000000000L );
    parent_bank->state = FD_BANK_STATE_REPLAYABLE; /* ParentReady can precede parent replay. */
    ulong window_seq = test_stem_seqs[ out_idx ];
    FD_TEST( !returnable_frag( ctx, TEST_VOTOR_IN_IDX, 0UL, FD_VOTOR_SIG_LEADER, in_chunk,
                               sizeof(fd_votor_msg_t), 0UL, 0UL, 0UL, test_stem ) );
    FD_TEST( test_stem_seqs[ out_idx ]==window_seq && !ctx->is_leader );
    FD_TEST( ctx->use_nominal_slot_duration==startup_nominal ); /* no reset accepted yet */
    long window_start = ctx->leader_window_start_ns;
    FD_TEST( window_start>=10000000000L );
    parent_bank->state = FD_BANK_STATE_FROZEN;

    /* Later slots inherit the same window.  The third slot is entered
       after its deadline; replay must not extend that deadline. */
    long const entry_delay_ns[ AG_SLOTS_PER_WINDOW ] = { 150000000L, 650000000L, 1250000000L, 1300000000L };
    ulong last_ns = 0UL;
    for( ulong i=0UL; i<AG_SLOTS_PER_WINDOW; i++ ) {
      ulong slot = AG_SLOTS_PER_WINDOW+i;
      int nominal = bam_ctrl.applied_enable;
      fd_clock_tile_set( ctx->clock, window_start+entry_delay_ns[ i ] );
      ctx->next_leader_slot = slot;
      *ctx->votor_leader = (fd_votor_leader_t){
        .slot            = slot,
        .parent_slot     = parent_bank->f.slot,
        .parent_block_id = parent_dmr,
      };

      ulong seq = test_stem_seqs[ out_idx ];
      FD_TEST( try_become_leader_ag( ctx, test_stem ) );
      FD_TEST( test_stem_seqs[ out_idx ]==seq+2UL );
      fd_frag_meta_t const * reset_meta  = test_stem_mcaches[ out_idx ] + fd_mcache_line_idx( seq,     test_stem_depths[ out_idx ] );
      fd_frag_meta_t const * leader_meta = test_stem_mcaches[ out_idx ] + fd_mcache_line_idx( seq+1UL, test_stem_depths[ out_idx ] );
      FD_TEST( reset_meta->sig==REPLAY_SIG_RESET && leader_meta->sig==REPLAY_SIG_BECAME_LEADER );
      fd_poh_reset_t const *     reset  = fd_chunk_to_laddr_const( ctx->replay_out->mem, reset_meta->chunk  );
      fd_became_leader_t const * leader = fd_chunk_to_laddr_const( ctx->replay_out->mem, leader_meta->chunk );
      fd_slot_params_t const * parent_params = &parent_bank->f.slot_params;
      fd_slot_params_t const *   params = &ctx->leader_bank->f.slot_params;
      ulong reset_ns  = nominal ? parent_params->ns_per_slot : parent_params->ns_per_slot_adjusted;
      ulong leader_ns = nominal ? params->ns_per_slot        : params->ns_per_slot_adjusted;
      FD_TEST( reset->tick_duration_ns==reset_ns/reset->ticks_per_slot );
      FD_TEST( leader->tick_duration_ns==leader_ns/leader->ticks_per_slot );
      FD_TEST( reset->tick_duration_ns==leader->tick_duration_ns );
      long deadline = window_start+(long)((i+1UL)*params->ns_per_slot)
                      -(nominal ? 0L : 50000000L)-6000000L;
      FD_TEST( leader->slot_end_ns==deadline );
      FD_TEST( ctx->leader_window_start_ns==window_start );
      FD_TEST( leader->slot_start_ns>=window_start+entry_delay_ns[ i ] );
      if( i==2UL ) FD_TEST( leader->slot_start_ns>leader->slot_end_ns );

      bam_ctrl.applied_enable = (uchar)!nominal;
      FD_TEST( !try_become_leader_ag( ctx, test_stem ) );
      FD_TEST( ctx->use_nominal_slot_duration==nominal );
      FD_TEST( ctx->metrics.slot_duration_ns==leader_ns );
      FD_TEST( leader->slot_end_ns==deadline );
      last_ns = leader_ns;

      /* Stand up the completed parent that the next slot's continuation
         uses, while retaining the original ParentReady timestamp. */
      parent_bank = ctx->leader_bank;
      parent_dmr = (fd_hash_t){ .ul = { 0xBB0000UL+slot } };
      fd_block_id_ele_t * parent_ele = &ctx->block_id_arr[ parent_bank->idx ];
      parent_ele->block_info = ag_block_id( slot, parent_dmr.uc );
      parent_ele->latest_mr  = parent_dmr;
      parent_ele->dmr        = parent_dmr;
      parent_ele->block_id_seen = 1;
      FD_TEST( fd_ag_block_id_map_ele_insert( ctx->ag_block_id_map, parent_ele, ctx->block_id_arr ) );
      fd_banks_mark_bank_frozen( parent_bank );
      ctx->leader_bank->refcnt--;
      ctx->leader_bank = NULL;
      ctx->is_leader   = 0;
    }

    /* A fresh window gets a fresh ParentReady clock and the new runtime
       mode.  The prior window's elapsed time must not carry over. */
    fd_clock_tile_set( ctx->clock, window_start+2000000000L );
    *notification = (fd_votor_leader_t){ .slot = 2UL*AG_SLOTS_PER_WINDOW, .parent_slot = parent_bank->f.slot, .parent_block_id = parent_dmr };
    ulong next_seq = test_stem_seqs[ out_idx ];
    FD_TEST( !returnable_frag( ctx, TEST_VOTOR_IN_IDX, 1UL, FD_VOTOR_SIG_LEADER, in_chunk,
                               sizeof(fd_votor_msg_t), 0UL, 0UL, 0UL, test_stem ) );
    FD_TEST( test_stem_seqs[ out_idx ]==next_seq+2UL && ctx->is_leader );
    FD_TEST( ctx->leader_window_start_ns>=window_start+2000000000L );
    fd_frag_meta_t const * next_meta = test_stem_mcaches[ out_idx ] + fd_mcache_line_idx( next_seq+1UL, test_stem_depths[ out_idx ] );
    fd_became_leader_t const * next_leader = fd_chunk_to_laddr_const( ctx->replay_out->mem, next_meta->chunk );
    fd_slot_params_t const * next_params = &ctx->leader_bank->f.slot_params;
    FD_TEST( ctx->use_nominal_slot_duration==!!bam_ctrl.applied_enable );
    FD_TEST( next_leader->slot_end_ns==ctx->leader_window_start_ns+(long)next_params->ns_per_slot
                                     -(bam_ctrl.applied_enable ? 0L : 50000000L)-6000000L );
    last_ns = bam_ctrl.applied_enable ? next_params->ns_per_slot : next_params->ns_per_slot_adjusted;
    FD_TEST( next_leader->tick_duration_ns==last_ns/next_leader->ticks_per_slot );

    /* Completing a block led by another validator advances reset_slot
       past the ParentReady block without a PoH reset.  The gauge keeps
       the duration replay last handed to PoH. */
    fd_hash_t         replayed_dmr = { .ul = { 0xC0FFEEUL } };
    fd_bank_t *       replayed     = add_block( ctx, fd_banks_root( ctx->banks ), 2UL*AG_SLOTS_PER_WINDOW+1UL, &replayed_dmr );
    fd_block_footer_t footer       = {0};
    mock_footer_finalize = 1;
    publish_slot_completed( ctx, test_stem, replayed, 0, 0 /* is_leader */, 0UL, 0UL, &footer );
    mock_footer_finalize = 0;
    FD_TEST( ctx->reset_slot==2UL*AG_SLOTS_PER_WINDOW+1UL );
    FD_TEST( ctx->metrics.slot_duration_ns==last_ns );
  }
  FD_LOG_NOTICE(( "pass: test_ag_bam_runtime_slot_timing_mode" ));
}
int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  char const * _page_sz = fd_env_strip_cmdline_cstr ( &argc, &argv, "--page-sz",  NULL, "gigantic"               );
  ulong        page_cnt = fd_env_strip_cmdline_ulong( &argc, &argv, "--page-cnt", NULL, 1UL                      );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( &argc, &argv, "--numa-idx", NULL, fd_shmem_numa_idx( 0UL ) );
  fd_wksp_t * wksp      = fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ), page_cnt, fd_shmem_cpu_idx( numa_idx ), "wksp", 0UL );
  FD_TEST( wksp );

  test_poh_slot_timing_mode( wksp );            fd_wksp_reset( wksp, 42U );
  test_ag_bam_runtime_slot_timing_mode( wksp );

  FD_TEST( mock_store_view_success_cnt==mock_store_view_release_cnt );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
