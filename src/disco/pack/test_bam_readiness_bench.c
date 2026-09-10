/* Explicit opt-in measurement of the actual Pack tile's indexed readiness
   check.  Reuse transaction fixtures and private helpers; the fake Pack is
   not used by this benchmark. */
#define main test_pack_tile_fixture_main
#include "test_pack_tile_bam.c"
#undef main

static void *
readiness_alloc( ulong align,
                  ulong size ) {
  void * mem = aligned_alloc( align, fd_ulong_align_up( size, align ) );
  FD_TEST( mem );
  fd_memset( mem, 0, size );
  return mem;
}

static ulong
readiness_quantile( ulong values[101],
                     ulong percentile ) {
  for( ulong i=1UL; i<101UL; i++ ) {
    ulong value=values[i], j=i;
    while( j && value<values[j-1UL] ) { values[j]=values[j-1UL]; j--; }
    values[j]=value;
  }
  return values[percentile];
}

static __attribute__((noinline)) ulong
readiness_attempt( fd_pack_ctx_t * ctx,
                    ulong           worker,
                    int             flags,
                    fd_txn_e_t *    out,
                    int *           ready_out ) {
  ulong hint;
  fd_txn_p_t const * candidate = fd_pack_peek_bundle_candidate( ctx->pack, 1, &hint, NULL );
  int ready = pack_tile_bam_candidate_ready( ctx, candidate );
  *ready_out = ready;
  if( ready>0 ) flags |= FD_PACK_SCHEDULE_BAM_READY;
  return fd_pack_schedule_next_microblock_with_bundle_hint( ctx->pack, 1500000UL, 0.75f,
                                                            worker, flags, hint, out );
}

/* Measure the callback itself as well as the isolated view above.  A held
   future head makes duplicate candidate/readiness work visible without
   including executor latency or repeatedly inserting transactions. */
static void
readiness_callback_bench( void ) {
  char const * names[5] = { "normal_empty", "bam_future", "bam_future_busy",
                           "bam_no_builder_busy", "bam_configured_busy" };
  for( ulong scenario=0UL; scenario<5UL; scenario++ ) {
    for( int crank=0; crank<2; crank++ ) {
      test_pack_callbacks_t e[1];
      test_pack_callbacks_new( e, FD_PACK_STRATEGY_BALANCED );
      fd_pack_ctx_t * ctx = e->h->ctx;
      test_pack_callbacks_leader( e, 104UL, 0 );
      ctx->slot_end_ns = fd_log_wallclock()+60000000000L;
      if( scenario ) {
        test_pack_callbacks_insert( e, 41U, scenario>=3UL ? 104UL : 105UL, 0 );
        test_pack_callbacks_insert( e, 42U, 104UL, 0 );
      } else {
        e->status = 0UL;
        ctx->bam_override_snapshot = 0;
      }
      ctx->crank->enabled = crank;
      if( scenario>=2UL ) ctx->execle_idle_bitset = 0UL;
      if( scenario==4UL ) {
        /* A nonzero, already-applied configuration exercises the ordinary
           generator no-op as a control for unavailable-builder handling. */
        fd_acct_addr_t addresses[5] = {0};
        for( ulong i=0UL; i<5UL; i++ ) addresses[i].b[0] = (uchar)(i+1UL);
        fd_bundle_crank_gen_init( ctx->crank->gen, &addresses[0], &addresses[1],
                                  &addresses[2], &addresses[3], "BEN", 0UL );
        *ctx->bam_fee_meta->commission_pubkey = addresses[4];
        ctx->bam_fee_meta->commission = 7UL;
        ctx->crank->prev_config->discriminator = 0x82ccfa1ee0aa0c9bUL;
        fd_bundle_crank_apply( ctx->crank->gen, ctx->crank->prev_config,
                               ctx->bam_fee_meta->commission_pubkey, ctx->crank->tip_receiver_owner,
                               ctx->crank->epoch, ctx->bam_fee_meta->commission );
      }
      ulong cancels = test_bundle_cancel_call_cnt;
      ulong samples[101];
      for( ulong sample=0UL; sample<=101UL; sample++ ) {
        ulong attempts = sample ? 1024UL : 8192UL;
        long begin = fd_tickcount();
        for( ulong i=0UL; i<attempts; i++ ) test_pack_callbacks_step( e );
        ulong ticks = (ulong)(fd_tickcount()-begin);
        if( sample ) samples[sample-1UL] = ticks;
      }
      FD_TEST( ctx->leader_slot==104UL && !ctx->drain_execle );
      FD_TEST( !test_pack_callbacks_dispatch_count( e ) && !ctx->pack_idx );
      FD_TEST( !ctx->bam_pending_result_cnt && !ctx->bam_scheduled_work_cnt );
      FD_TEST( ctx->bam_pending_work_cnt==(scenario ? 2UL : 0UL) );
      FD_TEST( fd_pack_avail_txn_cnt( ctx->pack )==(scenario ? 2UL : 0UL) );
      FD_TEST( !ctx->bam_candidate_identity_mismatch_cnt && !ctx->crank->ib_inserted );
      if( scenario<3UL || !crank ) FD_TEST( cancels==test_bundle_cancel_call_cnt );
      FD_TEST( !fd_pack_current_block_cost( ctx->pack ) );
      FD_TEST( !ctx->crank->metrics[1] && !ctx->crank->metrics[2] && !ctx->crank->metrics[3] );
      FD_TEST( ctx->crank->metrics[0]==((scenario==4UL && crank) ? 8192UL+101UL*1024UL : 0UL) );
      FD_LOG_NOTICE(( "BAM_CALLBACK_BENCH scenario=%s crank=%i p50_ticks=%.3f p95_ticks=%.3f p99_ticks=%.3f",
                       names[scenario], crank,
                       (double)readiness_quantile( samples, 50UL )/1024.0,
                       (double)readiness_quantile( samples, 95UL )/1024.0,
                       (double)readiness_quantile( samples, 99UL )/1024.0 ));
      test_pack_callbacks_delete( e );
    }
  }
}

int
main( int argc, char ** argv ) {
  fd_boot( &argc, &argv );
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );
  ulong const capacity = 1024UL;
  ulong const occupancy[3] = { 1UL, 64UL, 1024UL };
  ulong const warmup=8192UL, attempts_per_sample=1024UL;
  FD_LOG_NOTICE(( "BAM_READINESS_BENCH scope=real_pack_view_tile_indexed_readiness_schedule unit=fd_tickcount_ticks samples=101 attempts_per_sample=%lu warmup=%lu target_slot=105 leader_slot=104 capacity=%lu runtime_latency=0",
                   attempts_per_sample, warmup, capacity ));
  for( ulong depth_idx=0UL; depth_idx<3UL; depth_idx++ ) {
    fd_pack_ctx_t * ctx = readiness_alloc( alignof(fd_pack_ctx_t), sizeof(fd_pack_ctx_t) );
    ctx->leader_slot = 104UL;
    ctx->bam_min_admission_slot = 104UL;
    ctx->bam_override_snapshot = 1;
    ctx->bam_ownership_gen = 7U;
    ctx->pack_idx = 17U;
    ctx->bam_work_max = capacity;
    ctx->max_pending_transactions = capacity;
    ctx->bam_work = readiness_alloc( alignof(pack_bam_work_t), capacity*sizeof(pack_bam_work_t) );
    ctx->bam_sig_pool = readiness_alloc( alignof(pack_bam_sig_ele_t), capacity*FD_PACK_MAX_TXN_PER_BUNDLE*sizeof(pack_bam_sig_ele_t) );
    ctx->bam_result_queue = readiness_alloc( alignof(fd_bam_bundle_result_t), 2UL*capacity*sizeof(fd_bam_bundle_result_t) );
    ulong chain_cnt = pack_tile_bam_sig_map_chain_cnt( capacity );
    void * sig_map_mem = readiness_alloc( pack_bam_sig_map_align(), pack_bam_sig_map_footprint( chain_cnt ) );
    ctx->bam_sig_map = pack_bam_sig_map_join( pack_bam_sig_map_new( sig_map_mem, chain_cnt, 0UL ) );
    FD_TEST( ctx->bam_sig_map );
    fd_rng_t rng[1];
    FD_TEST( fd_rng_join( fd_rng_new( rng, 0U, 0UL ) ) );
    fd_pack_limits_t limits = {
      .max_cost_per_block=FD_PACK_MAX_COST_PER_BLOCK_LOWER_BOUND,
      .max_vote_cost_per_block=FD_PACK_MAX_VOTE_COST_PER_BLOCK_LOWER_BOUND,
      .max_write_cost_per_acct=FD_PACK_MAX_WRITE_COST_PER_ACCT_LOWER_BOUND,
      .max_data_bytes_per_block=FD_PACK_MAX_DATA_PER_BLOCK,
      .max_txn_per_microblock=FD_PACK_MAX_TXN_PER_BUNDLE,
      .max_microblocks_per_block=1024UL,
      .max_allocated_data_per_block=FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK,
    };
    void * pack_mem = readiness_alloc( fd_pack_align(), fd_pack_footprint( capacity, BUNDLE_META_SZ, 2UL, &limits ) );
    ctx->pack = fd_pack_join( fd_pack_new( pack_mem, capacity, BUNDLE_META_SZ, 2UL, &limits, NULL, 0UL, rng ) );
    FD_TEST( ctx->pack );
    fd_pack_set_initializer_bundles_ready( ctx->pack );
    fd_ed25519_sig_t head_signature;
    for( ulong i=0UL; i<occupancy[depth_idx]; i++ ) {
      fd_txn_e_t * bundle[1];
      fd_pack_insert_bundle_init( ctx->pack, bundle, 1UL );
      fd_txn_p_t invalid[1];
      test_pack_tile_make_d18_poc_txns( invalid, bundle[0]->txnp );
      bundle[0]->txnp->source_tpu = FD_TXN_M_TPU_SOURCE_BAM;
      bundle[0]->txnp->bam.seq_id = (uint)(1000UL+i);
      bundle[0]->txnp->bam.scheduler_gen = 9U;
      bundle[0]->txnp->bam.batch_idx = 0U;
      bundle[0]->txnp->bam.revert_on_error = 0U;
      uchar * signature = bundle[0]->txnp->payload+TXN(bundle[0]->txnp)->signature_off;
      ulong signature_prefix = 0xA0000000UL+i;
      fd_memcpy( signature, &signature_prefix, sizeof(signature_prefix) );
      fd_ed25519_sig_t copied_signature;
      fd_memcpy( copied_signature, signature, sizeof(copied_signature) );
      if( !i ) fd_memcpy( head_signature, signature, sizeof(head_signature) );
      ulong deleted;
      FD_TEST( fd_pack_insert_bundle_fini( ctx->pack, bundle, 1UL, 100UL, FD_PACK_IB_TYPE_NONE,
                                           NULL, &deleted, NULL )>=0 );
      FD_TEST( !deleted );
      /* Later current-slot batches must remain behind the future-slot head. */
      ulong target = i ? 104UL : 105UL;
      FD_TEST( pack_tile_append_bam_work( ctx, copied_signature, 0L, (uint)(1000UL+i), 9U,
                                          target, target, 100UL, 0U, 1U ) );
    }
    FD_TEST( fd_pack_avail_txn_cnt( ctx->pack )==occupancy[depth_idx] );
    FD_TEST( ctx->bam_work_cnt==occupancy[depth_idx] );
    for( ulong worker=0UL; worker<2UL; worker++ ) {
      int flags = FD_PACK_SCHEDULE_VOTE | FD_PACK_SCHEDULE_BAM_ONLY |
                  (worker ? FD_PACK_SCHEDULE_BAM_SINGLE : FD_PACK_SCHEDULE_BUNDLE);
      ulong samples[101];
      fd_txn_e_t output[1];
      for( ulong sample=0UL; sample<=101UL; sample++ ) {
        ulong attempts = sample ? attempts_per_sample : warmup;
        ulong scheduled=0UL;
        int ready_seen=0;
        FD_COMPILER_MFENCE();
        long begin = fd_tickcount();
        for( ulong i=0UL; i<attempts; i++ ) {
          int ready;
          scheduled += readiness_attempt( ctx, worker, flags, output, &ready );
          ready_seen |= ready;
        }
        FD_COMPILER_MFENCE();
        ulong ticks = (ulong)(fd_tickcount()-begin);
        FD_TEST( !ready_seen && !scheduled );
        if( sample ) samples[sample-1UL]=ticks;
      }
      double divisor = (double)attempts_per_sample;
      FD_LOG_NOTICE(( "BAM_READINESS_BENCH permission=%s queued_txns=%lu work_count=%lu p50_ticks=%.3f p95_ticks=%.3f p99_ticks=%.3f",
                       worker ? "restricted_single" : "full_bundle", occupancy[depth_idx], ctx->bam_work_cnt,
                       (double)readiness_quantile( samples, 50UL )/divisor,
                       (double)readiness_quantile( samples, 95UL )/divisor,
                       (double)readiness_quantile( samples, 99UL )/divisor ));
      ulong hint;
      fd_txn_p_t const * candidate = fd_pack_peek_bundle_candidate( ctx->pack, 1, &hint, NULL );
      FD_TEST( candidate && candidate->bam.seq_id==1000U );
      FD_TEST( !memcmp( fd_txn_get_signatures( TXN(candidate), candidate->payload ), head_signature, sizeof(head_signature) ) );
      FD_TEST( !pack_tile_bam_candidate_ready( ctx, candidate ) );
      FD_TEST( ctx->bam_work_cnt==occupancy[depth_idx] && ctx->bam_pending_work_cnt==occupancy[depth_idx] );
      FD_TEST( fd_pack_avail_txn_cnt( ctx->pack )==occupancy[depth_idx] );
      FD_TEST( !fd_pack_current_block_cost( ctx->pack ) && ctx->pack_idx==17U );
      FD_TEST( !ctx->bam_scheduled_work_cnt && !ctx->bam_pending_result_cnt && !ctx->bam_candidate_identity_mismatch_cnt );
      FD_TEST( !test_insert_fini_call_cnt && !test_delete_call_cnt ); /* no fake-Pack calls */
    }
    FD_TEST( fd_pack_delete( fd_pack_leave( ctx->pack ) )==pack_mem );
    free( pack_mem );
    fd_rng_delete( fd_rng_leave( rng ) );
    pack_bam_sig_map_delete( pack_bam_sig_map_leave( ctx->bam_sig_map ) );
    free( sig_map_mem );
    free( ctx->bam_work );
    free( ctx->bam_sig_pool );
    free( ctx->bam_result_queue );
    free( ctx );
  }
  readiness_callback_bench();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
