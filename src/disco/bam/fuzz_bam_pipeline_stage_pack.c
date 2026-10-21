#define fd_tile_pack bam_fuzz_pack_tile_unused
#include "../pack/fd_pack_tile.c"
#undef fd_tile_pack

#include "fuzz_bam_pipeline_stage.h"
#include "../../flamenco/runtime/fd_bank.h"
#include "../../flamenco/runtime/fd_slot_params.h"

#define BAM_FUZZ_PACK_IN_RESOLV_IDX   (0UL)
#define BAM_FUZZ_PACK_IN_REPLAY_IDX   (1UL)
#define BAM_FUZZ_PACK_MCACHE_DEPTH    (256UL)
#define BAM_FUZZ_PACK_PENDING_TXN_MAX (64UL)
#define BAM_FUZZ_PACK_EXECLE_CNT      (1UL)

struct bam_fuzz_pack {
  fd_pack_ctx_t   ctx[1];
  void *          storage;
  fd_rng_t        rng_mem[1];
  fd_rng_t *      rng;
  uchar           execle_busy_fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  uchar           bam_status_fseq_mem [ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  ulong *         execle_busy_fseq;
  bam_fuzz_link_t leader_in[1];
  ulong           leader_in_chunk;
  ulong           leader_seq;
  bam_fuzz_outs_t outs;
};

static fd_pack_out_ctx_t
bam_fuzz_pack_out( bam_fuzz_pack_t * h,
                   ulong             idx,
                   fd_wksp_t *       wksp,
                   ulong             mtu ) {
  bam_fuzz_link_t const * o = bam_fuzz_outs_add( &h->outs, idx, wksp, BAM_FUZZ_PACK_MCACHE_DEPTH, mtu );
  return (fd_pack_out_ctx_t){ .mem = o->mem, .chunk0 = o->chunk0, .wmark = o->wmark, .chunk = o->chunk0, .out_idx = idx };
}

bam_fuzz_pack_t *
bam_fuzz_pack_new( fd_wksp_t *             wksp,
                   bam_fuzz_link_t const * in,
                   bam_fuzz_link_t         out[ BAM_FUZZ_OUT_MAX ],
                   ulong **                execle_busy_fseq ) {
  bam_fuzz_pack_t * h = fd_wksp_alloc_laddr( wksp, alignof(bam_fuzz_pack_t), sizeof(bam_fuzz_pack_t), 1UL );
  FD_TEST( h );
  fd_memset( h, 0, sizeof(*h) );
  fd_pack_ctx_t * ctx = h->ctx;

  fd_pack_limits_t upper = {
    .max_cost_per_block           = FD_PACK_MAX_COST_PER_BLOCK_UPPER_BOUND,
    .max_vote_cost_per_block      = FD_PACK_MAX_VOTE_COST_PER_BLOCK_UPPER_BOUND,
    .max_write_cost_per_acct      = FD_PACK_MAX_WRITE_COST_PER_ACCT_UPPER_BOUND,
    .max_data_bytes_per_block     = FD_PACK_MAX_DATA_PER_BLOCK,
    .max_txn_per_microblock       = EFFECTIVE_TXN_PER_MICROBLOCK,
    .max_microblocks_per_block    = (ulong)UINT_MAX,
    .max_allocated_data_per_block = FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK,
  };
  fd_pack_limits_t lower = upper;
  lower.max_cost_per_block      = FD_PACK_MAX_COST_PER_BLOCK_LOWER_BOUND;
  lower.max_vote_cost_per_block = FD_PACK_MAX_VOTE_COST_PER_BLOCK_LOWER_BOUND;
  lower.max_write_cost_per_acct = FD_PACK_MAX_WRITE_COST_PER_ACCT_LOWER_BOUND;

  ulong pack_footprint = fd_pack_footprint( BAM_FUZZ_PACK_PENDING_TXN_MAX, BUNDLE_META_SZ, BAM_FUZZ_PACK_EXECLE_CNT, &upper );
  ulong sig_ele_cnt    = BAM_FUZZ_PACK_PENDING_TXN_MAX*FD_PACK_MAX_TXN_PER_BUNDLE;
  ulong sig_chain_cnt  = pack_tile_bam_sig_map_chain_cnt( BAM_FUZZ_PACK_PENDING_TXN_MAX );
  ulong result_cnt     = 2UL*BAM_FUZZ_PACK_PENDING_TXN_MAX;
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, fd_pack_align(),                pack_footprint );
  l = FD_LAYOUT_APPEND( l, alignof(pack_bam_work_t),        BAM_FUZZ_PACK_PENDING_TXN_MAX*sizeof(pack_bam_work_t) );
  l = FD_LAYOUT_APPEND( l, alignof(pack_bam_sig_ele_t),     sig_ele_cnt*sizeof(pack_bam_sig_ele_t) );
  l = FD_LAYOUT_APPEND( l, pack_bam_sig_map_align(),        pack_bam_sig_map_footprint( sig_chain_cnt ) );
  l = FD_LAYOUT_APPEND( l, alignof(fd_bam_bundle_result_t), result_cnt*sizeof(fd_bam_bundle_result_t) );
  ulong footprint = FD_LAYOUT_FINI( l, 4096UL );
  h->storage = fd_wksp_alloc_laddr( wksp, 4096UL, footprint, 1UL );
  FD_TEST( h->storage );

  h->rng = fd_rng_join( fd_rng_new( h->rng_mem, 0U, 3UL ) );
  FD_TEST( h->rng );

  FD_SCRATCH_ALLOC_INIT( alloc, h->storage );
  ctx->pack = fd_pack_join( fd_pack_new( FD_SCRATCH_ALLOC_APPEND( alloc, fd_pack_align(), pack_footprint ),
                                         BAM_FUZZ_PACK_PENDING_TXN_MAX, BUNDLE_META_SZ, BAM_FUZZ_PACK_EXECLE_CNT,
                                         &lower, NULL, 0UL, h->rng ) );
  FD_TEST( ctx->pack );
  ctx->bam_work         = FD_SCRATCH_ALLOC_APPEND( alloc, alignof(pack_bam_work_t), BAM_FUZZ_PACK_PENDING_TXN_MAX*sizeof(pack_bam_work_t) );
  ctx->bam_work_max     = BAM_FUZZ_PACK_PENDING_TXN_MAX;
  ctx->bam_sig_pool     = FD_SCRATCH_ALLOC_APPEND( alloc, alignof(pack_bam_sig_ele_t), sig_ele_cnt*sizeof(pack_bam_sig_ele_t) );
  ctx->bam_sig_map      = pack_bam_sig_map_join( pack_bam_sig_map_new(
                              FD_SCRATCH_ALLOC_APPEND( alloc, pack_bam_sig_map_align(), pack_bam_sig_map_footprint( sig_chain_cnt ) ),
                              sig_chain_cnt, 0UL ) );
  FD_TEST( ctx->bam_sig_map );
  ctx->bam_result_queue = FD_SCRATCH_ALLOC_APPEND( alloc, alignof(fd_bam_bundle_result_t), result_cnt*sizeof(fd_bam_bundle_result_t) );
  FD_TEST( FD_SCRATCH_ALLOC_FINI( alloc, 4096UL )<=(ulong)h->storage+footprint );

  h->execle_busy_fseq  = fd_fseq_join( fd_fseq_new( h->execle_busy_fseq_mem, ULONG_MAX ) );
  ctx->bam_status_fseq = fd_fseq_join( fd_fseq_new( h->bam_status_fseq_mem, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE ) );
  FD_TEST( h->execle_busy_fseq && ctx->bam_status_fseq );

  bam_fuzz_link_new( h->leader_in, wksp, 4UL, sizeof(fd_became_leader_t) );
  h->leader_in_chunk = h->leader_in->chunk0;

  ctx->execle_out[ 0 ] = bam_fuzz_pack_out( h, BAM_FUZZ_PACK_OUT_EXECLE,     wksp, MAX_MICROBLOCK_SZ              );
  ctx->poh_out         = bam_fuzz_pack_out( h, BAM_FUZZ_PACK_OUT_POH,        wksp, sizeof(fd_done_packing_t)      );
  ctx->bam_leader_out  = bam_fuzz_pack_out( h, BAM_FUZZ_PACK_OUT_BAM_LEADER, wksp, sizeof(fd_bam_leader_state_t)  );
  ctx->bam_result_out  = bam_fuzz_pack_out( h, BAM_FUZZ_PACK_OUT_BAM_RESULT, wksp, sizeof(fd_bam_bundle_result_t) );

  ctx->in_kind[ BAM_FUZZ_PACK_IN_RESOLV_IDX ] = IN_KIND_RESOLV;
  ctx->in     [ BAM_FUZZ_PACK_IN_RESOLV_IDX ] = (fd_pack_in_ctx_t){ .mem = in->mem, .chunk0 = in->chunk0, .wmark = in->wmark };
  ctx->in_kind[ BAM_FUZZ_PACK_IN_REPLAY_IDX ] = IN_KIND_REPLAY;
  ctx->in     [ BAM_FUZZ_PACK_IN_REPLAY_IDX ] = (fd_pack_in_ctx_t){ .mem = h->leader_in->mem, .chunk0 = h->leader_in->chunk0,
                                                                    .wmark = h->leader_in->wmark };

  /* Fields not set here are zero-initialized. */
  ctx->strategy                           = FD_PACK_STRATEGY_PERF;
  ctx->use_consumed_cus                   = 0;
  ctx->max_pending_transactions           = BAM_FUZZ_PACK_PENDING_TXN_MAX;
  ctx->leader_slot                        = ULONG_MAX;
  ctx->leader_bank_idx                    = ULONG_MAX;
  fd_clock_tile_init( ctx->clock );
  ctx->microblock_duration_ticks          = (ulong)(fd_tempo_tick_per_ns( NULL )*(double)MICROBLOCK_DURATION_NS + 0.5);
  ctx->limits.slot_max_cost               = lower.max_cost_per_block;
  ctx->limits.slot_max_vote_cost          = lower.max_vote_cost_per_block;
  ctx->limits.slot_max_write_cost_per_acct = lower.max_write_cost_per_acct;
  ctx->execle_cnt                         = BAM_FUZZ_PACK_EXECLE_CNT;
  ctx->execle_idle_bitset                 = fd_ulong_mask_lsb( (int)BAM_FUZZ_PACK_EXECLE_CNT );
  ctx->execle_current[ 0 ]                = h->execle_busy_fseq;
  ctx->execle_expect [ 0 ]                = ULONG_MAX;
  for( ulong i=0UL; i<FD_PACK_BAM_RECENT_SLOT_CNT; i++ ) ctx->bam_recent_slot[ i ].slot = ULONG_MAX;
  fd_histf_join( fd_histf_new( ctx->bam_work_rx_to_first_outcome_nanos, FD_MHIST_MIN( PACK, BAM_WORK_RX_TO_FIRST_OUTCOME_NANOS ),
                                                                       FD_MHIST_MAX( PACK, BAM_WORK_RX_TO_FIRST_OUTCOME_NANOS ) ) );
  fd_histf_join( fd_metrics_histf_new_ns( ctx->schedule_duration, FD_MHIST_EDGES_NS( PACK, MICROBLOCK_SCHEDULE_DURATION_NANOS ) ) );
  fd_histf_join( fd_metrics_histf_new_ns( ctx->no_sched_duration, FD_MHIST_EDGES_NS( PACK, MICROBLOCK_SCHEDULE_EMPTY_DURATION_NANOS ) ) );
  fd_histf_join( fd_metrics_histf_new_ns( ctx->insert_duration,   FD_MHIST_EDGES_NS( PACK, TXN_INSERT_DURATION_NANOS ) ) );
  fd_histf_join( fd_metrics_histf_new_ns( ctx->complete_duration, FD_MHIST_EDGES_NS( PACK, MICROBLOCK_COMPLETE_DURATION_NANOS ) ) );
  ctx->metric_state_begin         = fd_tickcount();
  ctx->last_bam_leader_state.slot = ULONG_MAX;

  fd_memcpy( out, h->outs.link, sizeof(h->outs.link) );
  *execle_busy_fseq = h->execle_busy_fseq;
  return h;
}

void
bam_fuzz_pack_delete( bam_fuzz_pack_t * h ) {
  FD_TEST( fd_pack_delete( fd_pack_leave( h->ctx->pack ) ) );
  FD_TEST( fd_rng_delete( fd_rng_leave( h->rng ) )==h->rng_mem );
  FD_TEST( fd_fseq_delete( fd_fseq_leave( h->execle_busy_fseq     ) )==h->execle_busy_fseq_mem );
  FD_TEST( fd_fseq_delete( fd_fseq_leave( h->ctx->bam_status_fseq ) )==h->bam_status_fseq_mem  );
  bam_fuzz_link_delete( h->leader_in );
  bam_fuzz_outs_delete( &h->outs );
  fd_wksp_free_laddr( h->storage );
  fd_wksp_free_laddr( h );
}

bam_fuzz_res_t
bam_fuzz_pack_set_leader_slot( bam_fuzz_pack_t * h,
                               ulong             slot,
                               void const *      bank,
                               ulong             bank_idx,
                               long              now_ns ) {
  bam_fuzz_res_t res = bam_fuzz_outs_begin( &h->outs );

  if( FD_UNLIKELY( slot==ULONG_MAX ) ) {
    if( FD_LIKELY( h->ctx->leader_slot!=ULONG_MAX ) ) {
      /* after_credit returns before the slot-end check while skip_cnt>0
         (set after every scheduled microblock).  Production Pack keeps
         iterating on a monotonic clock and ends the slot within
         skip_cnt+1 callbacks; a single call here can leave Pack leader,
         and later pumps rewind the clock below slot_end_ns, so Pack would
         schedule into the execle bank the harness already tore down. */
      fd_clock_tile_set( h->ctx->clock, h->ctx->slot_end_ns );
      ulong max_iter = (ulong)fd_long_max( h->ctx->skip_cnt, 0L ) + 1UL;
      /* With use_consumed_cus off, skip_cnt is one microblock's
         schedule_cnt, so a larger value means runaway state. */
      FD_TEST( max_iter<=MAX_TXN_PER_MICROBLOCK+1UL );
      for( ulong i=0UL; i<max_iter && h->ctx->leader_slot!=ULONG_MAX; i++ ) {
        int opt_poll_in = 1;
        int charge_busy = 0;
        after_credit( h->ctx, h->outs.stem, &opt_poll_in, &charge_busy );
      }
      FD_TEST( h->ctx->leader_slot==ULONG_MAX );
    }
    return bam_fuzz_outs_end( &h->outs, res );
  }

  fd_clock_tile_set( h->ctx->clock, now_ns );
  if( FD_LIKELY( h->ctx->leader_slot==slot ) ) return bam_fuzz_outs_end( &h->outs, res );

  fd_became_leader_t * became = fd_chunk_to_laddr( h->leader_in->mem, h->leader_in_chunk );
  fd_memset( became, 0, sizeof(fd_became_leader_t) );
  became->slot                      = slot;
  became->block_height              = bank ? ((fd_bank_t const *)bank)->f.block_height : slot;
  became->slot_start_ns             = now_ns;
  became->slot_end_ns               = now_ns + 100000000L;
  became->bank_idx                  = bank_idx;
  became->max_microblocks_in_slot   = 64UL;
  became->ticks_per_slot            = 64UL;
  became->tick_duration_ns          = 1000000UL;
  became->hashcnt_per_tick          = 1UL;
  became->limits.slot_max_cost      = FD_PACK_MAX_COST_PER_BLOCK_LOWER_BOUND;
  became->limits.slot_max_vote_cost = FD_PACK_MAX_VOTE_COST_PER_BLOCK_LOWER_BOUND;
  became->limits.slot_max_write_cost_per_acct      = FD_PACK_MAX_WRITE_COST_PER_ACCT_LOWER_BOUND;
  became->limits.slot_max_allocated_data_per_block = FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK;
  became->limits.slot_max_data_shreds              = FD_SLOT_PARAMS_400MS.max_shred_idx;

  during_frag( h->ctx, BAM_FUZZ_PACK_IN_REPLAY_IDX, h->leader_seq, REPLAY_SIG_BECAME_LEADER, h->leader_in_chunk, sizeof(fd_became_leader_t), 0UL );
  after_frag( h->ctx, BAM_FUZZ_PACK_IN_REPLAY_IDX, h->leader_seq, REPLAY_SIG_BECAME_LEADER, sizeof(fd_became_leader_t), 0UL, 0UL, h->outs.stem );
  /* The tip crank is disabled here; model an initializer found in good shape. */
  if( !h->ctx->crank->enabled ) fd_pack_set_initializer_bundles_ready( h->ctx->pack );
  h->leader_seq++;
  h->leader_in_chunk = fd_dcache_compact_next( h->leader_in_chunk, sizeof(fd_became_leader_t), h->leader_in->chunk0, h->leader_in->wmark );
  return bam_fuzz_outs_end( &h->outs, res );
}

bam_fuzz_res_t
bam_fuzz_pack_frag( bam_fuzz_pack_t *      h,
                    fd_frag_meta_t const * meta,
                    ulong                  seq,
                    long                   now_ns ) {
  bam_fuzz_res_t res = bam_fuzz_outs_begin( &h->outs );
  fd_clock_tile_set( h->ctx->clock, now_ns );
  during_frag( h->ctx, BAM_FUZZ_PACK_IN_RESOLV_IDX, seq, meta->sig, meta->chunk, meta->sz, meta->ctl );
  after_frag( h->ctx, BAM_FUZZ_PACK_IN_RESOLV_IDX, seq, meta->sig, meta->sz, meta->tsorig, meta->tspub, h->outs.stem );
  return bam_fuzz_outs_end( &h->outs, res );
}

bam_fuzz_res_t
bam_fuzz_pack_credit( bam_fuzz_pack_t * h,
                      long              now_ns ) {
  bam_fuzz_res_t res = bam_fuzz_outs_begin( &h->outs );
  /* Keep Pack on the harness timeline even after an expensive result-queue
     check; elapsed host time must not close a simulated leader slot. */
  fd_clock_tile_set( h->ctx->clock, now_ns );
  int charge_busy = 0;
  before_credit( h->ctx, h->outs.stem, &charge_busy );
  int opt_poll_in = 1;
  after_credit( h->ctx, h->outs.stem, &opt_poll_in, &charge_busy );
  return bam_fuzz_outs_end( &h->outs, res );
}
