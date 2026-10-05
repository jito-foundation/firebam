/* test_bam_replay_tile checks the replay tile's BAM slot timing: the
   PoH slot duration follows the BAM runtime mode, latched once per
   Tower reset or Alpenglow leader slot.  It reuses the upstream replay
   tile harness (mocks, setup_ctx, drive_become_leader, test_stem)
   without running its tests. */

#define _GNU_SOURCE
#include "../../flamenco/accdb/fd_accdb.h"

/* Only the alpenclock read is mocked when driving real motor-completion
   callbacks below.  Banks, Votor delivery, footer encoding and stem
   publication remain real; the inherited harness mocks runtime settlement. */
static int   test_reward_clock_read;
static ulong test_reward_parent_nanos;
#define fd_accdb_read_one(a,f,p) \
  (test_reward_clock_read ? ((fd_acc_t){ .lamports=1UL, .data_len=sizeof(ulong), .data=(uchar *)&test_reward_parent_nanos }) \
                          : (fd_accdb_read_one)((a),(f),(p)))
#define fd_accdb_unread_one(a,p) do { if( !test_reward_clock_read ) (fd_accdb_unread_one)((a),(p)); } while(0)

#define main test_replay_tile_main
#include "../../discof/replay/test_replay_tile.c"
#undef main
#undef fd_accdb_read_one
#undef fd_accdb_unread_one

static void
deliver_reward( fd_replay_tile_t * ctx,
                ulong              seq,
                ulong              slot,
                ulong              block_id,
                int                notar ) {
  ulong chunk = ctx->in[ TEST_VOTOR_IN_IDX ].chunk0;
  fd_votor_reward_t * reward = fd_chunk_to_laddr( ctx->in[ TEST_VOTOR_IN_IDX ].mem, chunk );
  *reward = (fd_votor_reward_t){ .slot = slot, .block_id = { .ul = { block_id } } };
  fd_bls_agg_null( &reward->agg_notar );
  fd_bls_agg_null( &reward->agg_skip );
  if( notar ) fd_bls_set_insert( reward->agg_notar.set, 3UL );
  FD_TEST( !returnable_frag( ctx, TEST_VOTOR_IN_IDX, seq, FD_VOTOR_SIG_REWARD, chunk,
                            sizeof(fd_votor_msg_t), 0UL, 0UL, 0UL, test_stem ) );
}

static void
deliver_leader( fd_replay_tile_t * ctx,
                ulong              seq,
                ulong              slot,
                ulong              parent_slot,
                fd_hash_t const *  parent_id ) {
  ulong chunk = ctx->in[ TEST_VOTOR_IN_IDX ].chunk0;
  fd_votor_leader_t * leader = fd_chunk_to_laddr( ctx->in[ TEST_VOTOR_IN_IDX ].mem, chunk );
  *leader = (fd_votor_leader_t){ .slot=slot, .parent_slot=parent_slot, .parent_block_id=*parent_id };
  FD_TEST( !returnable_frag( ctx, TEST_VOTOR_IN_IDX, seq, FD_VOTOR_SIG_LEADER, chunk,
                            sizeof(fd_votor_msg_t), 0UL, 0UL, 0UL, test_stem ) );
}

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
    fd_clock_tile_set( ctx->clock, window_start+entry_delay_ns[ 0 ] );
    ulong leader_seq = test_stem_seqs[ out_idx ];
    FD_TEST( try_become_leader_ag( ctx, test_stem ) );
    FD_TEST( test_stem_seqs[ out_idx ]==leader_seq+2UL );
    ulong last_ns = 0UL;
    for( ulong i=0UL; i<AG_SLOTS_PER_WINDOW; i++ ) {
      ulong slot = AG_SLOTS_PER_WINDOW+i;
      int nominal = !!bam_ctrl.applied_enable;
      FD_TEST( ctx->is_leader && ctx->leader_bank->f.slot==slot );
      fd_frag_meta_t const * reset_meta  = test_stem_mcaches[ out_idx ] + fd_mcache_line_idx( leader_seq,     test_stem_depths[ out_idx ] );
      fd_frag_meta_t const * leader_meta = test_stem_mcaches[ out_idx ] + fd_mcache_line_idx( leader_seq+1UL, test_stem_depths[ out_idx ] );
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

      /* Register the leader block's returned identity, then drive real
         leader completion and the automatic next-slot continuation. */
      parent_bank = ctx->leader_bank;
      parent_dmr = (fd_hash_t){ .ul = { 0xBB0000UL+slot } };
      fd_block_id_ele_t * parent_ele = &ctx->block_id_arr[ parent_bank->idx ];
      parent_ele->block_info = ag_block_id( slot, parent_dmr.uc );
      parent_ele->latest_mr  = parent_dmr;
      parent_ele->dmr        = parent_dmr;
      parent_ele->block_id_seen = 1;
      FD_TEST( fd_ag_block_id_map_ele_insert( ctx->ag_block_id_map, parent_ele, ctx->block_id_arr ) );
      if( i+1UL<AG_SLOTS_PER_WINDOW )
        fd_clock_tile_set( ctx->clock, window_start+entry_delay_ns[ i+1UL ] );
      ctx->recv_poh = 1;
      ulong completion_seq = test_stem_seqs[ out_idx ];
      mock_footer_finalize = 1;
      FD_TEST( try_fini_leader( ctx, test_stem ) );
      mock_footer_finalize = 0;
      FD_TEST( parent_bank->state==FD_BANK_STATE_FROZEN );
      FD_TEST( replay_out_sig( ctx, completion_seq )==REPLAY_SIG_SLOT_COMPLETED );
      if( i+1UL<AG_SLOTS_PER_WINDOW ) {
        FD_TEST( ctx->is_leader && test_stem_seqs[ out_idx ]==completion_seq+3UL );
        leader_seq = completion_seq+1UL;
      } else {
        FD_TEST( !ctx->is_leader && test_stem_seqs[ out_idx ]==completion_seq+1UL );
      }
    }

    /* A fresh window gets a fresh ParentReady clock and the new runtime
       mode.  The prior window's elapsed time must not carry over. */
    fd_clock_tile_set( ctx->clock, window_start+2000000000L );
    *notification = (fd_votor_leader_t){ .slot = 2UL*AG_SLOTS_PER_WINDOW, .parent_slot = parent_bank->f.slot, .parent_block_id = parent_dmr };
    ulong next_seq = test_stem_seqs[ out_idx ];
    FD_TEST( !returnable_frag( ctx, TEST_VOTOR_IN_IDX, 16UL, FD_VOTOR_SIG_LEADER, in_chunk,
                               sizeof(fd_votor_msg_t), 0UL, 0UL, 0UL, test_stem ) );
    FD_TEST( test_stem_seqs[ out_idx ]==next_seq && !ctx->is_leader );
    deliver_reward( ctx, 17UL, 0UL, 0UL, 0 );
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

    /* Votor can advance past a skipped window while the previous leader
       block is still completing.  Preserve that pending ParentReady and
       its anchor instead of automatically producing the stale next slot. */
    fd_bank_t * old_leader = ctx->leader_bank;
    long old_deadline = next_leader->slot_end_ns;
    int old_nominal = ctx->use_nominal_slot_duration;
    bam_ctrl.applied_enable = (uchar)!old_nominal;
    fd_clock_tile_set( ctx->clock, window_start+4000000000L );
    *notification = (fd_votor_leader_t){ .slot = 3UL*AG_SLOTS_PER_WINDOW, .parent_slot = parent_bank->f.slot, .parent_block_id = parent_dmr };
    ulong pending_seq = test_stem_seqs[ out_idx ];
    FD_TEST( !returnable_frag( ctx, TEST_VOTOR_IN_IDX, 32UL, FD_VOTOR_SIG_LEADER, in_chunk,
                               sizeof(fd_votor_msg_t), 0UL, 0UL, 0UL, test_stem ) );
    deliver_reward( ctx, 33UL, AG_SLOTS_PER_WINDOW, 0UL, 0 );
    long pending_start = ctx->leader_window_start_ns;
    FD_TEST( pending_start>=window_start+4000000000L && pending_start>old_deadline );
    FD_TEST( test_stem_seqs[ out_idx ]==pending_seq && ctx->is_leader && ctx->leader_bank==old_leader );
    FD_TEST( ctx->next_leader_slot==3UL*AG_SLOTS_PER_WINDOW );
    FD_TEST( ctx->use_nominal_slot_duration==old_nominal && ctx->metrics.slot_duration_ns==last_ns );
    FD_TEST( next_leader->slot_end_ns==old_deadline );

    fd_hash_t old_leader_dmr = { .ul = { 0xBC0000UL+(ulong)startup_nominal } };
    fd_block_id_ele_t * old_leader_ele = &ctx->block_id_arr[ old_leader->idx ];
    old_leader_ele->block_info = ag_block_id( old_leader->f.slot, old_leader_dmr.uc );
    old_leader_ele->latest_mr  = old_leader_dmr;
    old_leader_ele->dmr        = old_leader_dmr;
    old_leader_ele->block_id_seen = 1;
    FD_TEST( fd_ag_block_id_map_ele_insert( ctx->ag_block_id_map, old_leader_ele, ctx->block_id_arr ) );
    ctx->recv_poh = 1;
    mock_footer_finalize = 1;
    FD_TEST( try_fini_leader( ctx, test_stem ) );
    mock_footer_finalize = 0;
    FD_TEST( old_leader->state==FD_BANK_STATE_FROZEN );
    FD_TEST( replay_out_sig( ctx, pending_seq )==REPLAY_SIG_SLOT_COMPLETED );
    FD_TEST( test_stem_seqs[ out_idx ]==pending_seq+1UL && !ctx->is_leader );
    FD_TEST( ctx->next_leader_slot==3UL*AG_SLOTS_PER_WINDOW && ctx->leader_window_start_ns==pending_start );
    FD_TEST( ctx->votor_leader->parent_slot==parent_bank->f.slot && fd_hash_eq( &ctx->votor_leader->parent_block_id, &parent_dmr ) );

    FD_TEST( try_become_leader_ag( ctx, test_stem ) );
    FD_TEST( test_stem_seqs[ out_idx ]==pending_seq+3UL && ctx->is_leader );
    FD_TEST( ctx->leader_bank->f.slot==3UL*AG_SLOTS_PER_WINDOW && ctx->leader_bank->parent_idx==parent_bank->idx );
    FD_TEST( ctx->use_nominal_slot_duration==!!bam_ctrl.applied_enable );
    FD_TEST( replay_out_sig( ctx, pending_seq+1UL )==REPLAY_SIG_RESET );
    FD_TEST( replay_out_sig( ctx, pending_seq+2UL )==REPLAY_SIG_BECAME_LEADER );
    fd_frag_meta_t const * pending_reset_meta = test_stem_mcaches[ out_idx ] + fd_mcache_line_idx( pending_seq+1UL, test_stem_depths[ out_idx ] );
    fd_frag_meta_t const * pending_leader_meta = test_stem_mcaches[ out_idx ] + fd_mcache_line_idx( pending_seq+2UL, test_stem_depths[ out_idx ] );
    fd_poh_reset_t const * pending_reset = fd_chunk_to_laddr_const( ctx->replay_out->mem, pending_reset_meta->chunk );
    fd_became_leader_t const * pending_leader = fd_chunk_to_laddr_const( ctx->replay_out->mem, pending_leader_meta->chunk );
    fd_slot_params_t const * pending_params = &ctx->leader_bank->f.slot_params;
    last_ns = bam_ctrl.applied_enable ? pending_params->ns_per_slot : pending_params->ns_per_slot_adjusted;
    FD_TEST( pending_leader->slot_end_ns==pending_start+(long)pending_params->ns_per_slot
                                        -(bam_ctrl.applied_enable ? 0L : 50000000L)-6000000L );
    FD_TEST( pending_reset->tick_duration_ns==pending_leader->tick_duration_ns && pending_leader->tick_duration_ns==last_ns/pending_leader->ticks_per_slot );
    FD_TEST( next_leader->slot_end_ns==old_deadline );

    /* Completing a block led by another validator advances reset_slot
       past the ParentReady block without a PoH reset.  The gauge keeps
       the duration replay last handed to PoH. */
    fd_hash_t         replayed_dmr = { .ul = { 0xC0FFEEUL } };
    fd_bank_t *       replayed     = add_block( ctx, fd_banks_root( ctx->banks ), 3UL*AG_SLOTS_PER_WINDOW+1UL, &replayed_dmr );
    fd_block_footer_t footer       = {0};
    mock_footer_finalize = 1;
    publish_slot_completed( ctx, test_stem, replayed, 0, 0 /* is_leader */, 0UL, 0UL, &footer );
    mock_footer_finalize = 0;
    FD_TEST( ctx->reset_slot==3UL*AG_SLOTS_PER_WINDOW+1UL );
    FD_TEST( ctx->metrics.slot_duration_ns==last_ns );
  }
  FD_LOG_NOTICE(( "pass: test_ag_bam_runtime_slot_timing_mode" ));
}

static fd_bank_t *
setup_reward_ctx( fd_replay_tile_t * ctx,
                  fd_wksp_t *        wksp,
                  fd_hash_t const *  parent_id ) {
  fd_wksp_reset( wksp, 42U );
  setup_ctx( ctx, wksp );
  setup_ag_block_id_map( ctx, wksp, parent_id );
  setup_votor_input( ctx, wksp );
  ctx->alpenglow = 1;
  ctx->votor_final->slot = ULONG_MAX;
  mock_leader_for_slot_override = 1;
  mock_leader_schedule_loaded = 1;
  mock_slot_leader = ctx->identity_pubkey[ 0 ];
  fd_clock_tile_set( ctx->clock, 10000000000L );
  return fd_banks_root( ctx->banks );
}

/* Drive the real completed-motor callback and leader finalization.  The
   fixture supplies a clock account and bypasses unrelated account/runtime
   settlement, as the inherited replay harness does. */
static fd_bank_t *
complete_reward_leader( fd_replay_tile_t * ctx ) {
  fd_bank_t * bank = ctx->leader_bank;
  FD_TEST( bank );
  fd_hash_t dmr = { .ul = { 0xE000UL+bank->f.slot } };
  fd_block_id_ele_t * ele = &ctx->block_id_arr[ bank->idx ];
  ele->block_info    = ag_block_id( bank->f.slot, dmr.uc );
  ele->latest_mr     = dmr;
  ele->dmr           = dmr;
  ele->block_id_seen = 1;
  FD_TEST( fd_ag_block_id_map_ele_insert( ctx->ag_block_id_map, ele, ctx->block_id_arr ) );
  /* No genesis-certificate account is needed for this lifecycle probe. */
  bank->f.features.alpenglow = ULONG_MAX;
  test_reward_parent_nanos = 9000000000UL;
  test_reward_clock_read = 1;
  mock_snapshot_boot = 1;
  mock_footer_finalize = 1;
  fd_poh_leader_slot_ended_t ended = { .completed=1, .slot=bank->f.slot, .timing_table_idx=ULONG_MAX };
  process_poh_message( ctx, test_stem, &ended );
  FD_TEST( ctx->recv_poh );
  FD_TEST( ctx->leader_reward_window.reward[ bank->f.slot-ctx->leader_reward_window.slot ].frozen );
  FD_TEST( try_fini_leader( ctx, test_stem ) );
  mock_footer_finalize = 0;
  mock_snapshot_boot = 0;
  test_reward_clock_read = 0;
  FD_TEST( bank->state==FD_BANK_STATE_FROZEN );
  return bank;
}

static void
test_ag_reward_admission_and_pins( fd_wksp_t * wksp ) {
  static fd_replay_tile_t ctx[ 1 ];
  fd_hash_t parent_id = { .ul = { 0xD001UL } };
  fd_bank_t * parent = setup_reward_ctx( ctx, wksp, &parent_id );
  ulong out_idx = ctx->replay_out->idx;
  ulong used = fd_banks_pool_used_cnt( ctx->banks );
  ulong seq0 = test_stem_seqs[ out_idx ];
  *test_stem_min_cr_avail = 15UL;
  test_stem_cr_avail[ out_idx ] = 15UL;
  test_stem->cr_decrement_amount = 1UL;

  deliver_reward( ctx, 9UL, 0UL, 0xBADUL, 0 ); /* pre-LEADER is not causal */
  deliver_leader( ctx, 10UL, 8UL, 0UL, &parent_id );
  long anchor = ctx->leader_window_start_ns;
  FD_TEST( !ctx->is_leader && ctx->leader_bank==NULL && ctx->highwater_leader_slot==ULONG_MAX );
  FD_TEST( test_stem_seqs[ out_idx ]==seq0 && test_stem_cr_avail[ out_idx ]==15UL );
  FD_TEST( fd_banks_pool_used_cnt( ctx->banks )==used && parent->refcnt==0UL );
  deliver_reward( ctx, 11UL, 1UL, 0xD101UL, 1 ); /* another slot cannot admit8 */
  FD_TEST( !ctx->is_leader && test_stem_seqs[ out_idx ]==seq0 );
  fd_clock_tile_set( ctx->clock, anchor+150000000L );
  deliver_reward( ctx, 12UL, 0UL, 0xD100UL, 0 ); /* verified empty receipt qualifies */
  FD_TEST( ctx->is_leader && ctx->leader_bank->f.slot==8UL );
  FD_TEST( test_stem_seqs[ out_idx ]==seq0+2UL && test_stem_cr_avail[ out_idx ]==13UL );
  FD_TEST( ctx->leader_reward_window.valid && ctx->leader_reward_window.slot==8UL && ctx->leader_reward_window.seq==10UL );
  FD_TEST( ctx->leader_reward_window.start_ns==anchor && ctx->leader_window_start_ns==anchor );
  test_stem->cr_decrement_amount = 0UL;
  deliver_reward( ctx, 13UL, 2UL, 0xD102UL, 1 );
  deliver_reward( ctx, 14UL, 3UL, 0xD103UL, 1 );
  deliver_leader( ctx, 15UL, 8UL, 0UL, &parent_id ); /* consumed duplicate */
  FD_TEST( ctx->next_leader_slot==ULONG_MAX && ctx->leader_reward_window.seq==10UL && ctx->leader_window_start_ns==anchor );

  fd_clock_tile_set( ctx->clock, anchor+2000000000L );
  deliver_leader( ctx, 16UL, 20UL, 0UL, &parent_id );
  long future_anchor = ctx->leader_window_start_ns;
  FD_TEST( future_anchor>anchor && ctx->leader_reward_window.start_ns==anchor );
  for( ulong i=0UL; i<AG_SLOTS_PER_WINDOW; i++ ) deliver_reward( ctx, 17UL+i, 12UL+i, 0xF000UL+i, 1 );
  FD_TEST( ctx->votor_reward[ 0 ].msg.slot==13UL ); /* global ring evicted reward0 */
  FD_TEST( ctx->votor_reward[ 1 ].msg.slot==14UL ); /* and reward1 */
  deliver_reward( ctx, 21UL, 1UL, 0xD201UL, 1 ); /* refresh despite newer ring alias */
  FD_TEST( ctx->votor_reward[ 1 ].msg.slot==14UL );
  FD_TEST( ctx->leader_reward_window.reward[ 1 ].msg.block_id.ul[ 0 ]==0xD201UL );
  fd_block_footer_t footer = {0};
  construct_footer_certs( ctx, 9UL, 0UL, &footer );
  FD_TEST( footer.has_notar_reward_cert && footer.notar_reward_cert.slot==1UL && footer.notar_reward_cert.block_id.ul[ 0 ]==0xD201UL );

  /* Cancel only the future request while slot8 still owns its window.
     The existing leader-finish transition remains the authority for9. */
  ctx->next_leader_slot = ULONG_MAX;
  complete_reward_leader( ctx );
  FD_TEST( ctx->is_leader && ctx->leader_bank->f.slot==9UL );
  FD_TEST( ctx->votor_leader_seq==10UL && ctx->leader_window_start_ns==anchor );
  FD_TEST( ctx->leader_reward_window.reward[ 0 ].frozen );
  deliver_reward( ctx, 22UL, 0UL, 0xD300UL, 1 );
  FD_TEST( ctx->leader_reward_window.reward[ 0 ].msg.block_id.ul[ 0 ]==0xD100UL );
  complete_reward_leader( ctx );
  FD_TEST( ctx->is_leader && ctx->leader_bank->f.slot==10UL );
  complete_reward_leader( ctx );
  FD_TEST( ctx->is_leader && ctx->leader_bank->f.slot==11UL );
  complete_reward_leader( ctx );
  FD_TEST( !ctx->is_leader && ctx->next_leader_slot==ULONG_MAX );
  FD_LOG_NOTICE(( "pass: test_ag_reward_admission_and_pins" ));
}

static void
test_ag_reward_wait_cancel_and_parent_lifetime( fd_wksp_t * wksp ) {
  static fd_replay_tile_t ctx[ 1 ];
  fd_hash_t root_id = { .ul = { 0xD002UL } };
  fd_bank_t * root = setup_reward_ctx( ctx, wksp, &root_id );
  deliver_leader( ctx, 10UL, 8UL, 0UL, &root_id );
  deliver_reward( ctx, 11UL, 0UL, 0UL, 0 );
  complete_reward_leader( ctx );
  FD_TEST( !ctx->is_leader && ctx->next_leader_slot==9UL ); /* reward1 absent */
  ulong seq0 = test_stem_seqs[ ctx->replay_out->idx ];
  ctx->consensus_root_slot = 0UL;
  ctx->consensus_root = root_id;
  fd_hash_t newer_id = { .ul = { 0xD010UL } };
  fd_bank_t * newer = add_block( ctx, root, 10UL, &newer_id );
  deliver_certed( ctx, AG_CERT_KIND_FAST_FINAL, newer->f.slot, &newer_id );
  FD_TEST( ctx->next_leader_slot==ULONG_MAX && ctx->consensus_root_slot==10UL );
  deliver_reward( ctx, 12UL, 1UL, 0xD101UL, 1 );
  FD_TEST( !ctx->is_leader && ctx->next_leader_slot==ULONG_MAX && test_stem_seqs[ ctx->replay_out->idx ]==seq0 );

  root = setup_reward_ctx( ctx, wksp, &root_id );
  fd_hash_t parent_id = { .ul = { 0xD007UL } };
  fd_bank_t * parent = add_block( ctx, root, 7UL, &parent_id );
  deliver_leader( ctx, 20UL, 8UL, 7UL, &parent_id );
  FD_TEST( parent->refcnt==0UL && !ctx->is_leader );
  FD_TEST( fd_banks_get_evictable_bank( ctx->banks, root )==parent->idx );
  FD_TEST( parent->state==FD_BANK_STATE_PRUNABLE );
  seq0 = test_stem_seqs[ ctx->replay_out->idx ];
  deliver_reward( ctx, 21UL, 0UL, 0UL, 0 );
  FD_TEST( !ctx->is_leader && parent->refcnt==0UL && test_stem_seqs[ ctx->replay_out->idx ]==seq0 );
  FD_TEST( !try_become_leader_ag( ctx, test_stem ) );

  /* A receipt can become ready before parent replay.  The callback
     attempt fails safely; the ordinary credited retry later succeeds. */
  parent = setup_reward_ctx( ctx, wksp, &root_id );
  parent->state = FD_BANK_STATE_REPLAYABLE;
  deliver_leader( ctx, 30UL, 8UL, 0UL, &root_id );
  deliver_reward( ctx, 31UL, 0UL, 0UL, 0 );
  FD_TEST( !ctx->is_leader );
  parent->state = FD_BANK_STATE_FROZEN;
  parent->bank_seq++; /* stale block-id map must not match a recycled bank */
  FD_TEST( !try_become_leader_ag( ctx, test_stem ) );
  parent->bank_seq--;
  FD_TEST( try_become_leader_ag( ctx, test_stem ) );
  FD_TEST( ctx->is_leader && ctx->leader_bank->parent_idx==parent->idx );
  FD_LOG_NOTICE(( "pass: test_ag_reward_wait_cancel_and_parent_lifetime" ));
}

static void
test_ag_reward_sequence_wrap_and_unaligned_window( fd_wksp_t * wksp ) {
  static fd_replay_tile_t ctx[ 1 ];
  fd_hash_t parent_id = { .ul = { 0xD003UL } };
  setup_reward_ctx( ctx, wksp, &parent_id );
  deliver_leader( ctx, ULONG_MAX, 8UL, 0UL, &parent_id );
  deliver_reward( ctx, 0UL, 0UL, 0UL, 0 );
  FD_TEST( ctx->is_leader && ctx->leader_reward_window.seq==ULONG_MAX );
  FD_TEST( ctx->leader_reward_window.reward[ 0 ].valid && ctx->leader_reward_window.reward[ 0 ].seq==0UL );

  setup_reward_ctx( ctx, wksp, &parent_id );
  deliver_leader( ctx, 0UL, 9UL, 0UL, &parent_id );
  FD_TEST( ctx->votor_leader_valid && !ctx->is_leader );
  deliver_reward( ctx, 1UL, 1UL, 0UL, 0 );
  FD_TEST( ctx->is_leader && ctx->leader_bank->f.slot==9UL );
  complete_reward_leader( ctx );
  FD_TEST( !ctx->is_leader && ctx->next_leader_slot==10UL );
  deliver_reward( ctx, 2UL, 2UL, 0UL, 0 );
  FD_TEST( ctx->is_leader && ctx->leader_bank->f.slot==10UL && ctx->leader_reward_window.slot==9UL && ctx->votor_leader_seq==0UL );
  complete_reward_leader( ctx );
  FD_TEST( !ctx->is_leader && ctx->next_leader_slot==11UL );
  deliver_reward( ctx, 3UL, 3UL, 0UL, 0 );
  FD_TEST( ctx->is_leader && ctx->leader_bank->f.slot==11UL );
  complete_reward_leader( ctx );
  FD_TEST( !ctx->is_leader && ctx->next_leader_slot==ULONG_MAX );
  FD_LOG_NOTICE(( "pass: test_ag_reward_sequence_wrap_and_unaligned_window" ));
}
/* Bounded local callback measurement, excluding fixture allocation. This
   reports the actual replay callback/bank/cache/publication work; the
   inherited execution-prepare mock means it is not a cluster latency claim. */
static void
test_ag_reward_callback_latency( fd_wksp_t * wksp ) {
  static fd_replay_tile_t ctx[1];
  fd_hash_t parent_id = { .ul = { 0xD099UL } };
  long samples[32];
  for( ulong i=0UL; i<32UL; i++ ) {
    setup_reward_ctx( ctx, wksp, &parent_id );
    deliver_leader( ctx, 10UL, 8UL, 0UL, &parent_id );
    long start = fd_log_wallclock();
    deliver_reward( ctx, 11UL, 0UL, 0UL, 0 );
    samples[i] = fd_log_wallclock()-start;
    FD_TEST( ctx->is_leader );
  }
  for( ulong i=1UL; i<32UL; i++ ) {
    long value=samples[i]; ulong j=i;
    while( j && samples[j-1UL]>value ) { samples[j]=samples[j-1UL]; j--; }
    samples[j]=value;
  }
  FD_LOG_NOTICE(( "reward credited callback local32: median %ld ns, p95 %ld ns, max %ld ns (runtime prepare mocked)",
                   samples[16], samples[30], samples[31] ));
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
  test_ag_reward_admission_and_pins( wksp );
  test_ag_reward_wait_cancel_and_parent_lifetime( wksp );
  test_ag_reward_sequence_wrap_and_unaligned_window( wksp );
  test_ag_reward_callback_latency( wksp );

  FD_TEST( mock_store_view_success_cnt==mock_store_view_release_cnt );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
