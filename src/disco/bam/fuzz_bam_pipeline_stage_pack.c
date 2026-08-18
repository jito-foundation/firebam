#define FD_TILE_TEST 1
#define fd_tile_pack bam_fuzz_pack_tile_unused
#include "../pack/fd_pack_tile.c"
#undef fd_tile_pack

#include "fuzz_bam_pipeline_stage_pack.h"
#include "../../flamenco/runtime/fd_slot_params.h"

static void (* const bam_fuzz_pack_init_refs[])( fd_topo_t const *, fd_topo_tile_t const * ) __attribute__((unused)) = {
  privileged_init,
  unprivileged_init,
};

static ulong (* const bam_fuzz_pack_policy_refs[])( fd_topo_t const *, fd_topo_tile_t const *, ulong, struct sock_filter * ) __attribute__((unused)) = {
  populate_allowed_seccomp,
};

static ulong (* const bam_fuzz_pack_fds_refs[])( fd_topo_t const *, fd_topo_tile_t const *, ulong, int * ) __attribute__((unused)) = {
  populate_allowed_fds,
};

#define BAM_FUZZ_PACK_IN_RESOLV_IDX      (0UL)
#define BAM_FUZZ_PACK_IN_POH_IDX         (1UL)
#define BAM_FUZZ_PACK_OUT_EXECLE_IDX     (0UL)
#define BAM_FUZZ_PACK_OUT_POH_IDX        (1UL)
#define BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX (2UL)
#define BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX (3UL)
#define BAM_FUZZ_PACK_OUT_CNT            (4UL)
#define BAM_FUZZ_PACK_MCACHE_DEPTH       (256UL)
#define BAM_FUZZ_PACK_PENDING_TXN_MAX    (64UL)
#define BAM_FUZZ_PACK_EXECLE_CNT         (1UL)

struct bam_fuzz_pack {
  fd_wksp_t * wksp;

  fd_pack_ctx_t * ctx;

  void * storage_mem;

  void *        rng_mem;
  fd_rng_t *    rng;

  void *        execle_busy_fseq_mem;
  ulong *       execle_busy_fseq;
  void *        bam_status_fseq_mem;
  ulong *       bam_status_fseq;

  void *        leader_in_dcache_mem;
  uchar *       leader_in_dcache;
  ulong         leader_in_chunk;
  ulong         leader_seq;

  void *           execle_mcache_mem;
  fd_frag_meta_t * execle_mcache;
  void *           execle_dcache_mem;
  uchar *          execle_dcache;

  void *           poh_mcache_mem;
  fd_frag_meta_t * poh_mcache;
  void *           poh_dcache_mem;
  uchar *          poh_dcache;

  void *           bam_leader_mcache_mem;
  fd_frag_meta_t * bam_leader_mcache;
  void *           bam_leader_dcache_mem;
  uchar *          bam_leader_dcache;

  void *           bam_result_mcache_mem;
  fd_frag_meta_t * bam_result_mcache;
  void *           bam_result_dcache_mem;
  uchar *          bam_result_dcache;

  fd_stem_context_t stem[1];
  fd_frag_meta_t *  stem_mcaches[ BAM_FUZZ_PACK_OUT_CNT ];
  ulong             stem_seqs[ BAM_FUZZ_PACK_OUT_CNT ];
  ulong             stem_depths[ BAM_FUZZ_PACK_OUT_CNT ];
  ulong             stem_cr_avail[ BAM_FUZZ_PACK_OUT_CNT ];
  ulong             stem_min_cr_avail[1];
  int               stem_out_reliable[ BAM_FUZZ_PACK_OUT_CNT ];
};

bam_fuzz_pack_t *
bam_fuzz_pack_new( fd_wksp_t * wksp,
                   fd_wksp_t * in_mem,
                   ulong       in_chunk0,
                   ulong       in_wmark,
                   bam_fuzz_link_t * execle_out,
                   bam_fuzz_link_t * bam_leader_out,
                   bam_fuzz_link_t * bam_result_out,
                   ulong **          execle_busy_fseq ) {
  bam_fuzz_pack_t * h = fd_wksp_alloc_laddr( wksp, alignof(bam_fuzz_pack_t), sizeof(bam_fuzz_pack_t), 1UL );
  FD_TEST( h );
  fd_memset( h, 0, sizeof(*h) );
  h->wksp = wksp;

  fd_pack_limits_t upper = {
    .max_cost_per_block           = FD_PACK_MAX_COST_PER_BLOCK_UPPER_BOUND,
    .max_vote_cost_per_block      = FD_PACK_MAX_VOTE_COST_PER_BLOCK_UPPER_BOUND,
    .max_write_cost_per_acct      = FD_PACK_MAX_WRITE_COST_PER_ACCT_UPPER_BOUND,
    .max_data_bytes_per_block     = FD_PACK_MAX_DATA_PER_BLOCK,
    .max_txn_per_microblock       = EFFECTIVE_TXN_PER_MICROBLOCK,
    .max_microblocks_per_block    = (ulong)UINT_MAX,
    .max_allocated_data_per_block = FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK,
  };
  ulong pack_footprint = fd_pack_footprint( BAM_FUZZ_PACK_PENDING_TXN_MAX,
                                            BUNDLE_META_SZ,
                                            BAM_FUZZ_PACK_EXECLE_CNT,
                                            &upper );
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_pack_ctx_t), sizeof(fd_pack_ctx_t) );
  l = FD_LAYOUT_APPEND( l, fd_rng_align(),         fd_rng_footprint() );
  l = FD_LAYOUT_APPEND( l, fd_pack_align(),        pack_footprint );
#if FD_PACK_USE_EXTRA_STORAGE
  l = FD_LAYOUT_APPEND( l, extra_txn_deq_align(),  extra_txn_deq_footprint() );
#endif
  l = FD_LAYOUT_APPEND( l, alignof(pack_bam_work_t), BAM_FUZZ_PACK_PENDING_TXN_MAX*sizeof(pack_bam_work_t) );
  l = FD_LAYOUT_APPEND( l, alignof(fd_bam_bundle_result_t), 2UL*BAM_FUZZ_PACK_PENDING_TXN_MAX*sizeof(fd_bam_bundle_result_t) );
  l = FD_LAYOUT_APPEND( l, fd_fseq_align(),        fd_fseq_footprint() );
  l = FD_LAYOUT_APPEND( l, fd_fseq_align(),        fd_fseq_footprint() );
  l = FD_LAYOUT_APPEND( l, fd_dcache_align(),      fd_dcache_footprint( fd_dcache_req_data_sz( sizeof(fd_became_leader_t), 4UL, 1UL, 1 ), 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_mcache_align(),      fd_mcache_footprint( BAM_FUZZ_PACK_MCACHE_DEPTH, 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_dcache_align(),      fd_dcache_footprint( fd_dcache_req_data_sz( MAX_MICROBLOCK_SZ, BAM_FUZZ_PACK_MCACHE_DEPTH, 1UL, 1 ), 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_mcache_align(),      fd_mcache_footprint( BAM_FUZZ_PACK_MCACHE_DEPTH, 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_dcache_align(),      fd_dcache_footprint( fd_dcache_req_data_sz( sizeof(fd_done_packing_t), BAM_FUZZ_PACK_MCACHE_DEPTH, 1UL, 1 ), 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_mcache_align(),      fd_mcache_footprint( BAM_FUZZ_PACK_MCACHE_DEPTH, 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_dcache_align(),      fd_dcache_footprint( fd_dcache_req_data_sz( sizeof(fd_bam_leader_state_t), BAM_FUZZ_PACK_MCACHE_DEPTH, 1UL, 1 ), 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_mcache_align(),      fd_mcache_footprint( BAM_FUZZ_PACK_MCACHE_DEPTH, 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_dcache_align(),      fd_dcache_footprint( fd_dcache_req_data_sz( sizeof(fd_bam_bundle_result_t), BAM_FUZZ_PACK_MCACHE_DEPTH, 1UL, 1 ), 0UL ) );
  ulong storage_footprint = FD_LAYOUT_FINI( l, fd_dcache_align() );
  h->storage_mem = fd_wksp_alloc_laddr( wksp, fd_dcache_align(), storage_footprint, 1UL );
  FD_TEST( h->storage_mem );

  FD_SCRATCH_ALLOC_INIT( alloc, h->storage_mem );
  ulong leader_in_data_sz  = fd_dcache_req_data_sz( sizeof(fd_became_leader_t), 4UL, 1UL, 1 );
  ulong execle_data_sz     = fd_dcache_req_data_sz( MAX_MICROBLOCK_SZ, BAM_FUZZ_PACK_MCACHE_DEPTH, 1UL, 1 );
  ulong poh_data_sz        = fd_dcache_req_data_sz( sizeof(fd_done_packing_t), BAM_FUZZ_PACK_MCACHE_DEPTH, 1UL, 1 );
  ulong bam_leader_data_sz = fd_dcache_req_data_sz( sizeof(fd_bam_leader_state_t), BAM_FUZZ_PACK_MCACHE_DEPTH, 1UL, 1 );
  ulong bam_result_data_sz = fd_dcache_req_data_sz( sizeof(fd_bam_bundle_result_t), BAM_FUZZ_PACK_MCACHE_DEPTH, 1UL, 1 );

  h->ctx = FD_SCRATCH_ALLOC_APPEND( alloc, alignof(fd_pack_ctx_t), sizeof(fd_pack_ctx_t) );
  FD_TEST( h->ctx );
  fd_memset( h->ctx, 0, sizeof(fd_pack_ctx_t) );

  h->rng_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_rng_align(), fd_rng_footprint() );
  FD_TEST( h->rng_mem );
  h->rng = fd_rng_join( fd_rng_new( h->rng_mem, 0U, 3UL ) );
  FD_TEST( h->rng );

  fd_pack_limits_t lower = {
    .max_cost_per_block           = FD_PACK_MAX_COST_PER_BLOCK_LOWER_BOUND,
    .max_vote_cost_per_block      = FD_PACK_MAX_VOTE_COST_PER_BLOCK_LOWER_BOUND,
    .max_write_cost_per_acct      = FD_PACK_MAX_WRITE_COST_PER_ACCT_LOWER_BOUND,
    .max_data_bytes_per_block     = FD_PACK_MAX_DATA_PER_BLOCK,
    .max_txn_per_microblock       = EFFECTIVE_TXN_PER_MICROBLOCK,
    .max_microblocks_per_block    = (ulong)UINT_MAX,
    .max_allocated_data_per_block = FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK,
  };
  h->ctx->pack = fd_pack_join( fd_pack_new( FD_SCRATCH_ALLOC_APPEND( alloc, fd_pack_align(), pack_footprint ),
                                            BAM_FUZZ_PACK_PENDING_TXN_MAX,
                                            BUNDLE_META_SZ,
                                            BAM_FUZZ_PACK_EXECLE_CNT,
                                            &lower,
                                            NULL,
                                            0UL,
                                            h->rng ) );
  FD_TEST( h->ctx->pack );

#if FD_PACK_USE_EXTRA_STORAGE
  h->ctx->extra_txn_deq = extra_txn_deq_join( extra_txn_deq_new( FD_SCRATCH_ALLOC_APPEND( alloc,
                                                                                          extra_txn_deq_align(),
                                                                                          extra_txn_deq_footprint() ) ) );
  FD_TEST( h->ctx->extra_txn_deq );
#endif

  h->ctx->bam_work = FD_SCRATCH_ALLOC_APPEND( alloc,
                                              alignof(pack_bam_work_t),
                                              BAM_FUZZ_PACK_PENDING_TXN_MAX*sizeof(pack_bam_work_t) );
  FD_TEST( h->ctx->bam_work );
  h->ctx->bam_result_queue = FD_SCRATCH_ALLOC_APPEND( alloc,
                                                      alignof(fd_bam_bundle_result_t),
                                                      2UL*BAM_FUZZ_PACK_PENDING_TXN_MAX*sizeof(fd_bam_bundle_result_t) );
  FD_TEST( h->ctx->bam_result_queue );

  h->execle_busy_fseq_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_fseq_align(), fd_fseq_footprint() );
  h->execle_busy_fseq = fd_fseq_join( fd_fseq_new( h->execle_busy_fseq_mem, ULONG_MAX ) );
  FD_TEST( h->execle_busy_fseq );

  h->bam_status_fseq_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_fseq_align(), fd_fseq_footprint() );
  h->bam_status_fseq = fd_fseq_join( fd_fseq_new( h->bam_status_fseq_mem, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE ) );
  FD_TEST( h->bam_status_fseq );

  h->leader_in_dcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_dcache_align(), fd_dcache_footprint( leader_in_data_sz, 0UL ) );
  h->leader_in_dcache = fd_dcache_join( fd_dcache_new( h->leader_in_dcache_mem, leader_in_data_sz, 0UL ) );
  FD_TEST( h->leader_in_dcache );
  h->leader_in_chunk = fd_dcache_compact_chunk0( h->leader_in_dcache, h->leader_in_dcache );

  h->execle_mcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_mcache_align(), fd_mcache_footprint( BAM_FUZZ_PACK_MCACHE_DEPTH, 0UL ) );
  h->execle_mcache = fd_mcache_join( fd_mcache_new( h->execle_mcache_mem, BAM_FUZZ_PACK_MCACHE_DEPTH, 0UL, 0UL ) );
  FD_TEST( h->execle_mcache );
  h->execle_dcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_dcache_align(), fd_dcache_footprint( execle_data_sz, 0UL ) );
  h->execle_dcache = fd_dcache_join( fd_dcache_new( h->execle_dcache_mem, execle_data_sz, 0UL ) );
  FD_TEST( h->execle_dcache );

  h->poh_mcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_mcache_align(), fd_mcache_footprint( BAM_FUZZ_PACK_MCACHE_DEPTH, 0UL ) );
  h->poh_mcache = fd_mcache_join( fd_mcache_new( h->poh_mcache_mem, BAM_FUZZ_PACK_MCACHE_DEPTH, 0UL, 0UL ) );
  FD_TEST( h->poh_mcache );
  h->poh_dcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_dcache_align(), fd_dcache_footprint( poh_data_sz, 0UL ) );
  h->poh_dcache = fd_dcache_join( fd_dcache_new( h->poh_dcache_mem, poh_data_sz, 0UL ) );
  FD_TEST( h->poh_dcache );

  h->bam_leader_mcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_mcache_align(), fd_mcache_footprint( BAM_FUZZ_PACK_MCACHE_DEPTH, 0UL ) );
  h->bam_leader_mcache = fd_mcache_join( fd_mcache_new( h->bam_leader_mcache_mem, BAM_FUZZ_PACK_MCACHE_DEPTH, 0UL, 0UL ) );
  FD_TEST( h->bam_leader_mcache );
  h->bam_leader_dcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_dcache_align(), fd_dcache_footprint( bam_leader_data_sz, 0UL ) );
  h->bam_leader_dcache = fd_dcache_join( fd_dcache_new( h->bam_leader_dcache_mem, bam_leader_data_sz, 0UL ) );
  FD_TEST( h->bam_leader_dcache );

  h->bam_result_mcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_mcache_align(), fd_mcache_footprint( BAM_FUZZ_PACK_MCACHE_DEPTH, 0UL ) );
  h->bam_result_mcache = fd_mcache_join( fd_mcache_new( h->bam_result_mcache_mem, BAM_FUZZ_PACK_MCACHE_DEPTH, 0UL, 0UL ) );
  FD_TEST( h->bam_result_mcache );
  h->bam_result_dcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_dcache_align(), fd_dcache_footprint( bam_result_data_sz, 0UL ) );
  h->bam_result_dcache = fd_dcache_join( fd_dcache_new( h->bam_result_dcache_mem, bam_result_data_sz, 0UL ) );
  FD_TEST( h->bam_result_dcache );

  ulong storage_top = FD_SCRATCH_ALLOC_FINI( alloc, fd_dcache_align() );
  FD_TEST( storage_top <= (ulong)h->storage_mem + storage_footprint );

  h->stem_mcaches[ BAM_FUZZ_PACK_OUT_EXECLE_IDX     ] = h->execle_mcache;
  h->stem_mcaches[ BAM_FUZZ_PACK_OUT_POH_IDX        ] = h->poh_mcache;
  h->stem_mcaches[ BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX ] = h->bam_leader_mcache;
  h->stem_mcaches[ BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX ] = h->bam_result_mcache;
  for( ulong i=0UL; i<BAM_FUZZ_PACK_OUT_CNT; i++ ) {
    h->stem_depths[ i ] = BAM_FUZZ_PACK_MCACHE_DEPTH;
    h->stem_cr_avail[ i ] = ULONG_MAX;
    h->stem_out_reliable[ i ] = 0;
  }
  h->stem_min_cr_avail[0] = ULONG_MAX;
  *h->stem = (fd_stem_context_t) {
    .mcaches             = h->stem_mcaches,
    .seqs                = h->stem_seqs,
    .depths              = h->stem_depths,
    .cr_avail            = h->stem_cr_avail,
    .min_cr_avail        = h->stem_min_cr_avail,
    .cr_decrement_amount = 0UL,
    .out_reliable        = h->stem_out_reliable,
  };

  h->ctx->in_kind[ BAM_FUZZ_PACK_IN_RESOLV_IDX ] = IN_KIND_RESOLV;
  h->ctx->in[ BAM_FUZZ_PACK_IN_RESOLV_IDX ] = (fd_pack_in_ctx_t) {
    .mem    = in_mem,
    .chunk0 = in_chunk0,
    .wmark  = in_wmark,
  };
  h->ctx->in_kind[ BAM_FUZZ_PACK_IN_POH_IDX ] = IN_KIND_POH;
  h->ctx->in[ BAM_FUZZ_PACK_IN_POH_IDX ] = (fd_pack_in_ctx_t) {
    .mem    = (fd_wksp_t *)h->leader_in_dcache,
    .chunk0 = fd_dcache_compact_chunk0( h->leader_in_dcache, h->leader_in_dcache ),
    .wmark  = fd_dcache_compact_wmark ( h->leader_in_dcache, h->leader_in_dcache, sizeof(fd_became_leader_t) ),
  };

  h->ctx->cur_spot                      = NULL;
  h->ctx->bundle_kind                   = PACK_TILE_BUNDLE_KIND_NONE;
  h->ctx->dump_bam_mode                 = 0U;
  h->ctx->strategy                      = FD_PACK_STRATEGY_PERF;
  h->ctx->max_pending_transactions      = BAM_FUZZ_PACK_PENDING_TXN_MAX;
  h->ctx->leader_slot                   = ULONG_MAX;
  h->ctx->leader_bank                   = NULL;
  h->ctx->leader_bank_idx               = ULONG_MAX;
  h->ctx->pack_idx                      = 0UL;
  h->ctx->slot_microblock_cnt           = 0UL;
  h->ctx->pack_txn_cnt                  = 0UL;
  h->ctx->slot_max_microblocks          = 0UL;
  h->ctx->slot_dynamic_max_microblocks  = 0UL;
  h->ctx->pending_reduce_mb_bound       = 0;
  h->ctx->slot_max_data                 = 0UL;
  h->ctx->larger_shred_limits_per_block = 0;
  h->ctx->drain_execle                  = 0;
  h->ctx->rng                           = h->rng;
  fd_clock_tile_init( h->ctx->clock );
  h->ctx->last_successful_insert        = 0L;
  h->ctx->highest_observed_slot         = 0UL;
  h->ctx->bam_pending_check_slot        = 0UL;
  h->ctx->microblock_duration_ticks     = (ulong)(fd_tempo_tick_per_ns( NULL )*(double)MICROBLOCK_DURATION_NS + 0.5);
  h->ctx->use_consumed_cus              = 0;
  h->ctx->limits.slot_max_cost          = lower.max_cost_per_block;
  h->ctx->limits.slot_max_vote_cost     = lower.max_vote_cost_per_block;
  h->ctx->limits.slot_max_write_cost_per_acct = lower.max_write_cost_per_acct;
  h->ctx->execle_cnt                    = BAM_FUZZ_PACK_EXECLE_CNT;
  h->ctx->poll_cursor                   = 0;
  h->ctx->skip_cnt                      = 0L;
  h->ctx->execle_idle_bitset            = fd_ulong_mask_lsb( (int)BAM_FUZZ_PACK_EXECLE_CNT );
  h->ctx->execle_current[0]             = h->execle_busy_fseq;
  h->ctx->execle_expect[0]              = ULONG_MAX;
  h->ctx->execle_ready_at[0]            = 0L;

  h->ctx->execle_out[ 0 ] = (fd_pack_out_ctx_t) {
    .mem     = (fd_wksp_t *)h->execle_dcache,
    .chunk0  = fd_dcache_compact_chunk0( h->execle_dcache, h->execle_dcache ),
    .wmark   = fd_dcache_compact_wmark ( h->execle_dcache, h->execle_dcache, MAX_MICROBLOCK_SZ ),
    .chunk   = fd_dcache_compact_chunk0( h->execle_dcache, h->execle_dcache ),
    .out_idx = BAM_FUZZ_PACK_OUT_EXECLE_IDX,
  };

  h->ctx->poh_out = (fd_pack_out_ctx_t) {
    .mem     = (fd_wksp_t *)h->poh_dcache,
    .chunk0  = fd_dcache_compact_chunk0( h->poh_dcache, h->poh_dcache ),
    .wmark   = fd_dcache_compact_wmark ( h->poh_dcache, h->poh_dcache, sizeof(fd_done_packing_t) ),
    .chunk   = fd_dcache_compact_chunk0( h->poh_dcache, h->poh_dcache ),
    .out_idx = BAM_FUZZ_PACK_OUT_POH_IDX,
  };

  h->ctx->bam_leader_out = (pack_bam_out_ctx_t) {
    .idx    = BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX,
    .mem    = (fd_wksp_t *)h->bam_leader_dcache,
    .chunk0 = fd_dcache_compact_chunk0( h->bam_leader_dcache, h->bam_leader_dcache ),
    .wmark  = fd_dcache_compact_wmark ( h->bam_leader_dcache, h->bam_leader_dcache, sizeof(fd_bam_leader_state_t) ),
    .chunk  = fd_dcache_compact_chunk0( h->bam_leader_dcache, h->bam_leader_dcache ),
  };
  h->ctx->bam_result_out = (pack_bam_out_ctx_t) {
    .idx    = BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX,
    .mem    = (fd_wksp_t *)h->bam_result_dcache,
    .chunk0 = fd_dcache_compact_chunk0( h->bam_result_dcache, h->bam_result_dcache ),
    .wmark  = fd_dcache_compact_wmark ( h->bam_result_dcache, h->bam_result_dcache, sizeof(fd_bam_bundle_result_t) ),
    .chunk  = fd_dcache_compact_chunk0( h->bam_result_dcache, h->bam_result_dcache ),
  };
  h->ctx->bam_fee_cfg = NULL;
  h->ctx->bam_status_fseq = h->bam_status_fseq;

  memset( h->ctx->insert_result, '\0', sizeof(h->ctx->insert_result) );
  memset( h->ctx->bam_bundle_assembly_abandon_cnt, '\0', sizeof(h->ctx->bam_bundle_assembly_abandon_cnt) );
  memset( h->ctx->bam_work_rejected_pre_pending_cnt, '\0', sizeof(h->ctx->bam_work_rejected_pre_pending_cnt) );
  memset( h->ctx->bam_pending_work_evicted_cnt, '\0', sizeof(h->ctx->bam_pending_work_evicted_cnt) );
  memset( h->ctx->bam_work_item_stage_cnt, '\0', sizeof(h->ctx->bam_work_item_stage_cnt) );
  memset( h->ctx->bam_work_first_outcome_cnt, '\0', sizeof(h->ctx->bam_work_first_outcome_cnt) );
  for( ulong i=0UL; i<FD_PACK_BAM_RECENT_SLOT_CNT; i++ ) h->ctx->bam_recent_slot[ i ].slot = ULONG_MAX;
  h->ctx->bam_current_slot_has_bam_work = 0U;
  h->ctx->bam_first_insert_seen = 0U;
  h->ctx->bam_first_schedule_seen = 0U;
  h->ctx->bam_first_insert_minus_slot_end_ns = 0L;
  h->ctx->bam_first_schedule_minus_slot_end_ns = 0L;
  fd_histf_join( fd_histf_new( h->ctx->bam_work_rx_to_first_outcome_nanos,
                               FD_MHIST_MIN( PACK, BAM_WORK_RX_TO_FIRST_OUTCOME_NANOS ),
                               FD_MHIST_MAX( PACK, BAM_WORK_RX_TO_FIRST_OUTCOME_NANOS ) ) );
  fd_histf_join( fd_histf_new( h->ctx->schedule_duration, FD_MHIST_SECONDS_MIN( PACK, SCHEDULE_MICROBLOCK_DURATION_SECONDS ),
                                                         FD_MHIST_SECONDS_MAX( PACK, SCHEDULE_MICROBLOCK_DURATION_SECONDS ) ) );
  fd_histf_join( fd_histf_new( h->ctx->no_sched_duration, FD_MHIST_SECONDS_MIN( PACK, NO_SCHEDULE_MICROBLOCK_DURATION_SECONDS ),
                                                         FD_MHIST_SECONDS_MAX( PACK, NO_SCHEDULE_MICROBLOCK_DURATION_SECONDS ) ) );
  fd_histf_join( fd_histf_new( h->ctx->insert_duration,   FD_MHIST_SECONDS_MIN( PACK, INSERT_TRANSACTION_DURATION_SECONDS ),
                                                         FD_MHIST_SECONDS_MAX( PACK, INSERT_TRANSACTION_DURATION_SECONDS ) ) );
  fd_histf_join( fd_histf_new( h->ctx->complete_duration, FD_MHIST_SECONDS_MIN( PACK, COMPLETE_MICROBLOCK_DURATION_SECONDS ),
                                                         FD_MHIST_SECONDS_MAX( PACK, COMPLETE_MICROBLOCK_DURATION_SECONDS ) ) );
  h->ctx->metric_state = 0;
  h->ctx->metric_state_begin = fd_tickcount();
  memset( h->ctx->metric_timing, '\0', sizeof(h->ctx->metric_timing) );
  memset( h->ctx->current_bundle, '\0', sizeof(h->ctx->current_bundle) );
  memset( h->ctx->current_bundle_bam, '\0', sizeof(h->ctx->current_bundle_bam) );
  memset( h->ctx->blk_engine_cfg, '\0', sizeof(h->ctx->blk_engine_cfg) );
  memset( h->ctx->last_sched_metrics, '\0', sizeof(h->ctx->last_sched_metrics) );
  memset( h->ctx->start_block_sched_metrics, '\0', sizeof(h->ctx->start_block_sched_metrics) );
  memset( h->ctx->crank, '\0', sizeof(h->ctx->crank) );
  h->ctx->last_bam_leader_state.slot = ULONG_MAX;

  if( FD_LIKELY( execle_out ) ) {
    *execle_out = (bam_fuzz_link_t) {
      .mem    = (fd_wksp_t *)h->execle_dcache,
      .mcache = h->execle_mcache,
      .chunk0 = h->ctx->execle_out[ 0 ].chunk0,
      .wmark  = h->ctx->execle_out[ 0 ].wmark,
      .depth  = BAM_FUZZ_PACK_MCACHE_DEPTH,
    };
  }
  if( FD_LIKELY( bam_leader_out ) ) {
    *bam_leader_out = (bam_fuzz_link_t) {
      .mem    = (fd_wksp_t *)h->bam_leader_dcache,
      .mcache = h->bam_leader_mcache,
      .chunk0 = h->ctx->bam_leader_out.chunk0,
      .wmark  = h->ctx->bam_leader_out.wmark,
      .depth  = BAM_FUZZ_PACK_MCACHE_DEPTH,
    };
  }
  if( FD_LIKELY( bam_result_out ) ) {
    *bam_result_out = (bam_fuzz_link_t) {
      .mem    = (fd_wksp_t *)h->bam_result_dcache,
      .mcache = h->bam_result_mcache,
      .chunk0 = h->ctx->bam_result_out.chunk0,
      .wmark  = h->ctx->bam_result_out.wmark,
      .depth  = BAM_FUZZ_PACK_MCACHE_DEPTH,
    };
  }
  if( FD_LIKELY( execle_busy_fseq ) ) *execle_busy_fseq = h->execle_busy_fseq;

  return h;
}

void
bam_fuzz_pack_delete( bam_fuzz_pack_t * h ) {
  if( FD_UNLIKELY( !h ) ) return;

  if( h->ctx && h->ctx->pack ) {
    void * mem = fd_pack_delete( fd_pack_leave( h->ctx->pack ) );
    FD_TEST( mem );
    h->ctx->pack = NULL;
  }
  if( h->rng ) {
    void * mem = fd_rng_delete( fd_rng_leave( h->rng ) );
    FD_TEST( mem==h->rng_mem );
    h->rng = NULL;
    h->rng_mem = NULL;
  }
  bam_fuzz_delete_fseq  ( &h->execle_busy_fseq, h->execle_busy_fseq_mem );
  bam_fuzz_delete_fseq  ( &h->bam_status_fseq,  h->bam_status_fseq_mem  );
  bam_fuzz_delete_dcache( &h->leader_in_dcache, h->leader_in_dcache_mem );
  bam_fuzz_delete_mcache( &h->execle_mcache,    h->execle_mcache_mem    );
  bam_fuzz_delete_dcache( &h->execle_dcache,    h->execle_dcache_mem    );
  bam_fuzz_delete_mcache( &h->poh_mcache,       h->poh_mcache_mem       );
  bam_fuzz_delete_dcache( &h->poh_dcache,       h->poh_dcache_mem       );
  bam_fuzz_delete_mcache( &h->bam_leader_mcache, h->bam_leader_mcache_mem );
  bam_fuzz_delete_dcache( &h->bam_leader_dcache, h->bam_leader_dcache_mem );
  bam_fuzz_delete_mcache( &h->bam_result_mcache, h->bam_result_mcache_mem );
  bam_fuzz_delete_dcache( &h->bam_result_dcache, h->bam_result_dcache_mem );

  if( h->storage_mem ) {
    fd_wksp_free_laddr( h->storage_mem );
    h->storage_mem = NULL;
  }
  fd_wksp_free_laddr( h );
}

bam_fuzz_pack_result_t
bam_fuzz_pack_set_leader_slot( bam_fuzz_pack_t * h,
                               ulong             slot,
                               void const *      bank,
                               ulong             bank_idx,
                               long              now_ns ) {
  bam_fuzz_pack_result_t res = {
    .execle_before      = h->stem_seqs[ BAM_FUZZ_PACK_OUT_EXECLE_IDX ],
    .execle_after       = h->stem_seqs[ BAM_FUZZ_PACK_OUT_EXECLE_IDX ],
    .poh_before         = h->stem_seqs[ BAM_FUZZ_PACK_OUT_POH_IDX ],
    .poh_after          = h->stem_seqs[ BAM_FUZZ_PACK_OUT_POH_IDX ],
    .bam_leader_before  = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX ],
    .bam_leader_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX ],
    .bam_result_before  = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX ],
    .bam_result_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX ],
    .pending_work_cnt   = h->ctx->bam_pending_work_cnt,
    .scheduled_work_cnt = h->ctx->bam_scheduled_work_cnt,
  };

  if( FD_UNLIKELY( slot==ULONG_MAX ) ) {
    if( FD_LIKELY( h->ctx->leader_slot!=ULONG_MAX ) ) {
      fd_clock_tile_set( h->ctx->clock, h->ctx->slot_end_ns );
      int opt_poll_in = 1;
      int charge_busy = 0;
      after_credit( h->ctx, h->stem, &opt_poll_in, &charge_busy );
    }
    res.execle_after       = h->stem_seqs[ BAM_FUZZ_PACK_OUT_EXECLE_IDX ];
    res.poh_after          = h->stem_seqs[ BAM_FUZZ_PACK_OUT_POH_IDX ];
    res.bam_leader_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX ];
    res.bam_result_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX ];
    res.pending_work_cnt   = h->ctx->bam_pending_work_cnt;
    res.scheduled_work_cnt = h->ctx->bam_scheduled_work_cnt;
    return res;
  }

  if( FD_LIKELY( h->ctx->leader_slot==slot ) ) {
    res.execle_after       = h->stem_seqs[ BAM_FUZZ_PACK_OUT_EXECLE_IDX ];
    res.poh_after          = h->stem_seqs[ BAM_FUZZ_PACK_OUT_POH_IDX ];
    res.bam_leader_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX ];
    res.bam_result_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX ];
    res.pending_work_cnt   = h->ctx->bam_pending_work_cnt;
    res.scheduled_work_cnt = h->ctx->bam_scheduled_work_cnt;
    return res;
  }

  fd_became_leader_t * became = fd_chunk_to_laddr( h->leader_in_dcache, h->leader_in_chunk );
  fd_memset( became, 0, sizeof(fd_became_leader_t) );
  became->slot                      = slot;
  became->slot_start_ns             = now_ns;
  became->slot_end_ns               = now_ns + 100000000L;
  became->bank                      = bank;
  became->bank_idx                  = bank_idx;
  became->max_microblocks_in_slot   = 64UL;
  became->ticks_per_slot            = 64UL;
  became->tick_duration_ns          = 1000000UL;
  became->total_skipped_ticks       = 0UL;
  became->hashcnt_per_tick          = 1UL;
  became->epoch                     = 0UL;
  became->limits.slot_max_cost      = FD_PACK_MAX_COST_PER_BLOCK_LOWER_BOUND;
  became->limits.slot_max_vote_cost = FD_PACK_MAX_VOTE_COST_PER_BLOCK_LOWER_BOUND;
  became->limits.slot_max_write_cost_per_acct = FD_PACK_MAX_WRITE_COST_PER_ACCT_LOWER_BOUND;
  became->limits.slot_max_allocated_data_per_block = FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK;
  became->limits.slot_max_data_shreds              = FD_SLOT_PARAMS_400MS.max_shred_idx;
  fd_clock_tile_set( h->ctx->clock, now_ns );

  ulong sig = fd_disco_poh_sig( slot, POH_PKT_TYPE_BECAME_LEADER, 0UL );
  during_frag( h->ctx,
               BAM_FUZZ_PACK_IN_POH_IDX,
               h->leader_seq,
               sig,
               h->leader_in_chunk,
               sizeof(fd_became_leader_t),
               0UL );
  after_frag( h->ctx,
              BAM_FUZZ_PACK_IN_POH_IDX,
              h->leader_seq,
              sig,
              sizeof(fd_became_leader_t),
              0UL,
              0UL,
              h->stem );
  h->leader_seq++;
  h->leader_in_chunk = fd_dcache_compact_next( h->leader_in_chunk,
                                               sizeof(fd_became_leader_t),
                                               fd_dcache_compact_chunk0( h->leader_in_dcache, h->leader_in_dcache ),
                                               fd_dcache_compact_wmark( h->leader_in_dcache,
                                                                        h->leader_in_dcache,
                                                                        sizeof(fd_became_leader_t) ) );

  res.execle_after       = h->stem_seqs[ BAM_FUZZ_PACK_OUT_EXECLE_IDX ];
  res.poh_after          = h->stem_seqs[ BAM_FUZZ_PACK_OUT_POH_IDX ];
  res.bam_leader_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX ];
  res.bam_result_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX ];
  res.pending_work_cnt   = h->ctx->bam_pending_work_cnt;
  res.scheduled_work_cnt = h->ctx->bam_scheduled_work_cnt;
  return res;
}

bam_fuzz_pack_result_t
bam_fuzz_pack_frag( bam_fuzz_pack_t *    h,
                    fd_frag_meta_t const * meta,
                    ulong                seq ) {
  bam_fuzz_pack_result_t res = {
    .execle_before      = h->stem_seqs[ BAM_FUZZ_PACK_OUT_EXECLE_IDX ],
    .execle_after       = h->stem_seqs[ BAM_FUZZ_PACK_OUT_EXECLE_IDX ],
    .poh_before         = h->stem_seqs[ BAM_FUZZ_PACK_OUT_POH_IDX ],
    .poh_after          = h->stem_seqs[ BAM_FUZZ_PACK_OUT_POH_IDX ],
    .bam_leader_before  = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX ],
    .bam_leader_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX ],
    .bam_result_before  = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX ],
    .bam_result_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX ],
    .pending_work_cnt   = h->ctx->bam_pending_work_cnt,
    .scheduled_work_cnt = h->ctx->bam_scheduled_work_cnt,
  };
  during_frag( h->ctx, BAM_FUZZ_PACK_IN_RESOLV_IDX, seq, meta->sig, meta->chunk, meta->sz, meta->ctl );
  after_frag( h->ctx,
              BAM_FUZZ_PACK_IN_RESOLV_IDX,
              seq,
              meta->sig,
              meta->sz,
              meta->tsorig,
              meta->tspub,
              h->stem );
  res.execle_after       = h->stem_seqs[ BAM_FUZZ_PACK_OUT_EXECLE_IDX ];
  res.poh_after          = h->stem_seqs[ BAM_FUZZ_PACK_OUT_POH_IDX ];
  res.bam_leader_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX ];
  res.bam_result_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX ];
  res.pending_work_cnt   = h->ctx->bam_pending_work_cnt;
  res.scheduled_work_cnt = h->ctx->bam_scheduled_work_cnt;
  return res;
}

bam_fuzz_pack_result_t
bam_fuzz_pack_credit( bam_fuzz_pack_t * h ) {
  bam_fuzz_pack_result_t res = {
    .execle_before      = h->stem_seqs[ BAM_FUZZ_PACK_OUT_EXECLE_IDX ],
    .execle_after       = h->stem_seqs[ BAM_FUZZ_PACK_OUT_EXECLE_IDX ],
    .poh_before         = h->stem_seqs[ BAM_FUZZ_PACK_OUT_POH_IDX ],
    .poh_after          = h->stem_seqs[ BAM_FUZZ_PACK_OUT_POH_IDX ],
    .bam_leader_before  = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX ],
    .bam_leader_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX ],
    .bam_result_before  = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX ],
    .bam_result_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX ],
    .pending_work_cnt   = h->ctx->bam_pending_work_cnt,
    .scheduled_work_cnt = h->ctx->bam_scheduled_work_cnt,
  };
  int charge_busy = 0;
  before_credit( h->ctx, h->stem, &charge_busy );
  int opt_poll_in = 1;
  after_credit( h->ctx, h->stem, &opt_poll_in, &charge_busy );
  res.execle_after       = h->stem_seqs[ BAM_FUZZ_PACK_OUT_EXECLE_IDX ];
  res.poh_after          = h->stem_seqs[ BAM_FUZZ_PACK_OUT_POH_IDX ];
  res.bam_leader_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_LEADER_IDX ];
  res.bam_result_after   = h->stem_seqs[ BAM_FUZZ_PACK_OUT_BAM_RESULT_IDX ];
  res.pending_work_cnt   = h->ctx->bam_pending_work_cnt;
  res.scheduled_work_cnt = h->ctx->bam_scheduled_work_cnt;
  return res;
}
