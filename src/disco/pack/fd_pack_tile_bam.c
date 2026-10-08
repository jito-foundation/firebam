/* FireBAM part of the pack tile.  Included once by fd_pack_tile.c
   after fd_pack_ctx_t and update_metric_state; the types are in
   fd_pack_tile_bam.h.  remove_ib is defined right after this file. */

static inline void remove_ib( fd_pack_ctx_t * ctx );

static inline ulong
pack_tile_bam_pending_work_cnt( fd_pack_ctx_t const * ctx ) {
  return ctx->bam_work_cnt-ctx->bam_scheduled_work_cnt;
}

static inline long
pack_tile_wallclock_from_ticks( fd_pack_ctx_t const * ctx,
                                long                  now_ticks ) {
  return fd_clock_tile_tickcount_to_wallclock( ctx->clock, now_ticks );
}

static inline void
pack_tile_publish_bam_leader_state( fd_pack_ctx_t *     ctx,
                                    fd_stem_context_t * stem,
                                    long                now_ticks ) {
  if( FD_UNLIKELY( ctx->leader_slot==ULONG_MAX || ctx->bam_leader_out.out_idx==ULONG_MAX ) ) return;

  /* Slot and fresh-work transitions feed BAM's local health state and must be
     visible immediately. All other updates are latest-value-wins and sampled
     at the reference validator's 5 ms cadence. */
  if( FD_LIKELY( ctx->leader_slot==ctx->last_bam_leader_state.slot &&
                 ctx->bam_current_slot_has_bam_work==ctx->last_bam_leader_state.current_slot_has_bam_work &&
                 now_ticks<ctx->bam_leader_state_next_publish_ticks ) ) return;

  long interval_ticks = (long)((double)PACK_BAM_LEADER_STATE_INTERVAL_NS*ctx->clock->epoch->w)+1L;
  ctx->bam_leader_state_next_publish_ticks = fd_long_sat_add( now_ticks, interval_ticks );

  /* The shared tile clock projects tickcount deltas into wallclock
     nanoseconds without calling fd_log_wallclock on this hot path. */
  long now_ns    = pack_tile_wallclock_from_ticks( ctx, now_ticks );
  fd_became_leader_t const * became_leader = ctx->_became_leader;
  /* BAM leader-state tick uses the same slot-relative notion as pack's
     reference_tick. Clamp pre-slot and post-slot observations into the
     valid [0,ticks_per_slot] range because now_ns is derived from an
     approximate wallclock sample taken during housekeeping. */
  uint tick = FD_LIKELY( became_leader->tick_duration_ns && now_ns>became_leader->slot_start_ns )
            ? fd_uint_min( (uint)((now_ns - became_leader->slot_start_ns) / (long)became_leader->tick_duration_ns),
                           (uint)became_leader->ticks_per_slot )
            : 0U;

  fd_bam_leader_state_t state = { .slot = ctx->leader_slot, .tick = (ushort)fd_uint_min( tick, (uint)USHORT_MAX ),
    .slot_cu_budget_remaining = (uint)fd_ulong_sat_sub( ctx->limits.slot_max_cost, fd_pack_current_block_cost( ctx->pack ) ),
    .slot_end_ns = ctx->slot_end_ns,
    .current_slot_has_bam_work = (uchar)ctx->bam_current_slot_has_bam_work
  };

  if( FD_LIKELY( fd_bam_leader_state_eq( &state, &ctx->last_bam_leader_state ) ) ) return;
  ctx->last_bam_leader_state = state;

  fd_bam_leader_state_t * out = fd_chunk_to_laddr( ctx->bam_leader_out.mem, ctx->bam_leader_out.chunk );
  *out = state;

  fd_stem_publish( stem,
                   ctx->bam_leader_out.out_idx,
                   0UL,
                   ctx->bam_leader_out.chunk,
                   sizeof(fd_bam_leader_state_t),
                   0UL,
                   0UL,
                   fd_frag_meta_ts_comp( now_ticks ) );
  ctx->bam_leader_out.chunk = fd_dcache_compact_next( ctx->bam_leader_out.chunk,
                                                      sizeof(fd_bam_leader_state_t),
                                                      ctx->bam_leader_out.chunk0,
                                                      ctx->bam_leader_out.wmark );
}

static inline void
pack_tile_note_bam_first_outcome( fd_pack_ctx_t * ctx,
                                  ulong           outcome_idx,
                                  long            first_rx_ts_ns,
                                  long            outcome_ns ) {
  ctx->bam_work_first_outcome_cnt[ outcome_idx ]++;
  if( FD_LIKELY( first_rx_ts_ns>0L && outcome_ns>=first_rx_ts_ns ) ) {
    fd_histf_sample( ctx->bam_work_rx_to_first_outcome_nanos,
                     fd_ulong_sat_sub( (ulong)outcome_ns, (ulong)first_rx_ts_ns ) );
  }
}

static inline long
pack_tile_current_bam_bundle_first_rx_ts_ns( fd_pack_ctx_t const * ctx ) {
  long first_rx_ts_ns = 0L;
  for( ulong i=0UL; i<ctx->current_bundle->txn_received; i++ ) {
    long rx_ts_ns = ctx->current_bundle->bundle[ i ]->txnp->first_seen_nanos;
    if( FD_UNLIKELY( !first_rx_ts_ns || ( rx_ts_ns && rx_ts_ns<first_rx_ts_ns ) ) ) first_rx_ts_ns = rx_ts_ns;
  }
  return first_rx_ts_ns;
}

static inline void
pack_tile_note_first_bam_insert( fd_pack_ctx_t *     ctx,
                                 fd_stem_context_t * stem,
                                 long                now_ns,
                                 ulong               max_schedule_slot ) {
  if( FD_UNLIKELY( ctx->leader_slot==ULONG_MAX ) ) return;

  long insert_minus_slot_end_ns = now_ns - ctx->slot_end_ns;
  if( FD_UNLIKELY( !ctx->bam_first_insert_seen ) ) {
    ctx->bam_first_insert_seen = 1U;
    ctx->bam_first_insert_minus_slot_end_ns = insert_minus_slot_end_ns;
  }
  if( FD_LIKELY( ctx->bam_current_slot_has_bam_work || insert_minus_slot_end_ns>=0L || max_schedule_slot<ctx->leader_slot ) ) return;
  ctx->bam_current_slot_has_bam_work = 1U;
  pack_tile_publish_bam_leader_state( ctx, stem, fd_tickcount() );
}

static inline ulong
pack_tile_bam_first_event_result_idx( uchar seen,
                                      long  event_minus_slot_end_ns ) {
  if( FD_UNLIKELY( !seen ) ) return FD_METRICS_ENUM_PACK_BAM_FIRST_EVENT_RESULT_V_NO_EVENT_IDX;
  if( FD_LIKELY( event_minus_slot_end_ns<0L ) ) return FD_METRICS_ENUM_PACK_BAM_FIRST_EVENT_RESULT_V_BEFORE_END_IDX;
  return FD_METRICS_ENUM_PACK_BAM_FIRST_EVENT_RESULT_V_AFTER_END_IDX;
}

static inline int
pack_tile_enqueue_bam_result( fd_pack_ctx_t *               ctx,
                              fd_bam_bundle_result_t const * res ) {
  ulong result_queue_cap = 2UL*ctx->bam_work_max;
  if( FD_UNLIKELY( ctx->bam_pending_result_cnt >= result_queue_cap ) ) {
    FD_MCNT_INC( BAM, FEEDBACK_RESULTS_DROPPED, 1UL );
    FD_LOG_WARNING(( "dropping BAM result because pack result queue is full seq_id=%u slot=%lu pending_results=%lu cap=%lu",
                     res->seq_id, res->slot, ctx->bam_pending_result_cnt, result_queue_cap ));
    return 0;
  }
  /* Both terms are below capacity, so the sum wraps at most once.
     Use a compare instead of a runtime modulo. */
  ulong queue_idx = ctx->bam_result_queue_head + ctx->bam_pending_result_cnt;
  if( FD_UNLIKELY( queue_idx>=result_queue_cap ) ) queue_idx -= result_queue_cap;
  ctx->bam_result_queue[ queue_idx ] = *res;
  ctx->bam_pending_result_cnt++;
  return 1;
}

static inline fd_bam_bundle_result_t
pack_tile_make_bam_outside_slot_result( uint   seq_id,
                                        ushort scheduler_gen,
                                        ulong  max_schedule_slot,
                                        uchar  txn_cnt ) {
  fd_bam_bundle_result_t res = fd_bam_result_base( seq_id, scheduler_gen, max_schedule_slot, txn_cnt );
  res.scheduling_error = FD_BAM_SCHED_ERR_OUTSIDE_SLOT;
  return res;
}

static inline char const *
pack_tile_bam_pack_insert_reason_cstr( int pack_rc ) {
  switch( pack_rc ) {
  case FD_PACK_INSERT_REJECT_DUPLICATE:        return "insert_reject_duplicate";
  case FD_PACK_INSERT_REJECT_UNAFFORDABLE:     return "insert_reject_unaffordable";
  case FD_PACK_INSERT_REJECT_ADDR_LUT:         return "insert_reject_addr_lut";
  case FD_PACK_INSERT_REJECT_TOO_LARGE:        return "insert_reject_too_large";
  case FD_PACK_INSERT_REJECT_ACCOUNT_CNT:      return "insert_reject_account_cnt";
  case FD_PACK_INSERT_REJECT_DUPLICATE_ACCT:   return "insert_reject_duplicate_acct";
  case FD_PACK_INSERT_REJECT_ESTIMATION_FAIL:  return "insert_reject_estimation_fail";
  case FD_PACK_INSERT_REJECT_WRITES_SYSVAR:    return "insert_reject_writes_sysvar";
  case FD_PACK_INSERT_REJECT_BUNDLE_BLACKLIST: return "insert_reject_bundle_blacklist";
  case FD_PACK_INSERT_REJECT_ACCT_BLOCKLIST:    return "insert_reject_acct_blocklist";
  case FD_PACK_INSERT_REJECT_NONCE_CONFLICT:   return "insert_reject_nonce_conflict";
  case FD_PACK_INSERT_REJECT_PRIORITY:         return "insert_reject_container_full";
  case FD_PACK_INSERT_REJECT_NONCE_PRIORITY:   return "insert_reject_nonce_container_full";
  default:                                     return "insert_reject_other";
  }
}

/* A zero invalid reason or pack result means that diagnostic is unavailable.
   Ordered BAM assembly makes txn_received the first missing member index. */
static inline void
pack_tile_log_bam_drop( fd_pack_ctx_t const * ctx,
                        char const *          category,
                        char const *          reason,
                        uint                  invalid_reason_idx,
                        int                   pack_rc,
                        uint                  seq_id,
                        uchar                 txn_cnt,
                        char const *          work_state,
                        ulong                 max_schedule_slot,
                        ulong                 blockhash_height,
                        long                  first_rx_ts_ns,
                        _Bool                 revert_on_error_known,
                        _Bool                 revert_on_error,
                        ulong                 txn_received,
                        uint                  first_missing_idx_known,
                        void const *          sig0 ) {
  if( FD_LIKELY( ctx->dump_bam_mode!=FD_BAM_DEBUG_DUMP_MODE_ALL ) ) {
    if( FD_LIKELY( ctx->dump_bam_mode!=FD_BAM_DEBUG_DUMP_MODE_SLOT_FIRST || max_schedule_slot==ULONG_MAX ) ) return;
    pack_bam_recent_slot_t const * entry = &ctx->bam_recent_slot[ max_schedule_slot & ( FD_PACK_BAM_RECENT_SLOT_CNT - 1UL ) ];
    if( FD_UNLIKELY( entry->slot!=max_schedule_slot || entry->first_debug_seq_id!=seq_id ) ) return;
  }

  ulong validation_slot                    = ctx->leader_slot;
  ulong required_min_slot                  = ULONG_MAX;
  long  now_ns                             = pack_tile_wallclock_from_ticks( ctx, fd_tickcount() );
  ulong age_ns                             = ULONG_MAX;
  long  current_leader_slot_end_ns         = ctx->leader_slot==ULONG_MAX ? 0L : ctx->slot_end_ns;
  long  now_minus_current_leader_slot_end  = current_leader_slot_end_ns ? now_ns - current_leader_slot_end_ns : 0L;
  ulong pack_avail_txn_cnt                 = fd_pack_avail_txn_cnt( ctx->pack );
  ulong extra_queue_cnt                    = 0UL;
#if FD_PACK_USE_EXTRA_STORAGE
  extra_queue_cnt = extra_txn_deq_cnt( ctx->extra_txn_deq );
#endif
  if( FD_LIKELY( first_rx_ts_ns>0L && now_ns>=first_rx_ts_ns ) ) age_ns = (ulong)( now_ns - first_rx_ts_ns );
  if( FD_LIKELY( validation_slot!=ULONG_MAX ) ) required_min_slot = validation_slot;
  if( ctx->bam_min_admission_slot ) required_min_slot = fd_ulong_max( ctx->bam_min_admission_slot,
                                                                                  required_min_slot==ULONG_MAX ? 0UL : required_min_slot );

  char sig0_b58[ FD_BASE58_ENCODED_64_SZ ] = "<none>";
  if( FD_LIKELY( sig0 ) ) fd_base58_encode_64( (uchar const *)sig0, NULL, sig0_b58 );

  FD_LOG_INFO(( "bam_drop category=%s reason=%s invalid_reason_known=%u invalid_reason_idx=%u pack_rc_known=%u pack_rc=%d seq_id=%u txns=%u sig0=%s work_state=%s validation_slot=%lu validation_slot_known=%u required_min_slot=%lu required_min_slot_known=%u bam_max_schedule_slot=%lu work_slot=%lu work_slot_known=%u blockhash_height=%lu blockhash_height_known=%u leader_slot=%lu leader_slot_known=%u current_leader_slot_end_ns=%ld current_leader_slot_end_known=%u now_ns=%ld now_minus_current_leader_slot_end_ns=%ld highest_observed_block_height=%lu first_rx_ts_ns=%ld first_rx_known=%u age_ns=%lu age_known=%u revert_on_error_known=%u revert_on_error=%u batch_idx_known=%u batch_idx=%u txn_received=%lu txn_expected=%lu first_missing_idx_known=%u first_missing_idx=%u pack_avail_txn_cnt=%lu extra_queue_cnt=%lu bam_work_cnt=%lu bam_pending_work_cnt=%lu bam_scheduled_work_cnt=%lu max_pending_transactions=%lu",
                category,
                reason,
                (uint)( invalid_reason_idx!=0U ),
                invalid_reason_idx,
                (uint)( pack_rc<0 ),
                pack_rc,
                seq_id,
                (uint)txn_cnt,
                sig0_b58,
                work_state,
                validation_slot,
                (uint)( validation_slot!=ULONG_MAX ),
                required_min_slot,
                (uint)( required_min_slot!=ULONG_MAX ),
                max_schedule_slot,
                max_schedule_slot,
                (uint)( max_schedule_slot!=ULONG_MAX ),
                blockhash_height,
                (uint)( blockhash_height!=ULONG_MAX ),
                ctx->leader_slot,
                (uint)( ctx->leader_slot!=ULONG_MAX ),
                current_leader_slot_end_ns,
                (uint)( !!current_leader_slot_end_ns ),
                now_ns,
                now_minus_current_leader_slot_end,
                ctx->highest_observed_block_height,
                first_rx_ts_ns,
                (uint)( first_rx_ts_ns>0L ),
                age_ns,
                (uint)( age_ns!=ULONG_MAX ),
                (uint)revert_on_error_known,
                (uint)revert_on_error,
                0U,
                0U,
                txn_received,
                (ulong)txn_cnt,
                first_missing_idx_known,
                first_missing_idx_known ? (uint)txn_received : 0U,
                pack_avail_txn_cnt,
                extra_queue_cnt,
                ctx->bam_work_cnt,
                pack_tile_bam_pending_work_cnt( ctx ),
                ctx->bam_scheduled_work_cnt,
                ctx->max_pending_transactions ));
}

/* Reject invalid or missed targets.  As in jito-solana, a future target
   is ready in the current leader slot.  Resolver reference slots are not
   fork-qualified validity. */
static inline int
pack_tile_bam_target_expired( fd_pack_ctx_t const * ctx,
                              ulong                 max_schedule_slot ) {
  if( FD_UNLIKELY( max_schedule_slot==ULONG_MAX || max_schedule_slot<ctx->bam_min_admission_slot ) ) return 1;
  return ctx->leader_slot!=ULONG_MAX && max_schedule_slot<ctx->leader_slot;
}

static inline void
pack_tile_bam_index_remove_one( fd_pack_ctx_t * ctx,
                                ulong           work_idx,
                                ulong           txn_idx ) {
  pack_bam_work_t * work = &ctx->bam_work[ work_idx ];
  if( FD_UNLIKELY( !(work->indexed_mask & (uchar)(1U<<txn_idx)) ) ) return;
  pack_bam_sig_map_idx_remove_fast( ctx->bam_sig_map, work_idx*FD_PACK_MAX_TXN_PER_BUNDLE + txn_idx, ctx->bam_sig_pool );
  work->indexed_mask &= (uchar)~(1U<<txn_idx);
}

static inline void
pack_tile_bam_index_insert_work( fd_pack_ctx_t * ctx,
                                 ulong           work_idx,
                                 uchar           mask ) {
  pack_bam_work_t * work = &ctx->bam_work[ work_idx ];
  for( ulong txn_idx=0UL; txn_idx<work->txn_cnt; txn_idx++ ) {
    if( !(mask & (uchar)(1U<<txn_idx)) ) continue;
    FD_TEST( !(work->indexed_mask & (uchar)(1U<<txn_idx)) );
    ulong ele_idx = work_idx*FD_PACK_MAX_TXN_PER_BUNDLE + txn_idx;
    pack_bam_sig_ele_t * ele = &ctx->bam_sig_pool[ ele_idx ];
    ele->key      = fd_ulong_load_8( work->sig[ txn_idx ] );
    ele->work_idx = (uint)work_idx;
    ele->txn_idx  = (uchar)txn_idx;
    pack_bam_sig_map_idx_insert( ctx->bam_sig_map, ele_idx, ctx->bam_sig_pool );
    work->indexed_mask |= (uchar)(1U<<txn_idx);
  }
}

static inline void
pack_tile_bam_index_remove_work( fd_pack_ctx_t * ctx,
                                 ulong           work_idx ) {
  pack_bam_work_t const * work = &ctx->bam_work[ work_idx ];
  for( ulong j=0UL; j<work->txn_cnt; j++ ) pack_tile_bam_index_remove_one( ctx, work_idx, j );
}

/* Move work and reindex its signatures. Unless dst==src, dst must be
   unindexed. */

static inline void
pack_tile_bam_work_move( fd_pack_ctx_t * ctx,
                         ulong           dst,
                         ulong           src ) {
  FD_TEST( !ctx->bam_work[ dst ].indexed_mask || dst==src );
  if( FD_UNLIKELY( dst==src ) ) return;
  uchar mask = ctx->bam_work[ src ].indexed_mask;
  pack_tile_bam_index_remove_work( ctx, src );
  ctx->bam_work[ dst ] = ctx->bam_work[ src ];
  pack_tile_bam_index_insert_work( ctx, dst, mask );
}

static inline void
pack_tile_bam_work_swap_remove( fd_pack_ctx_t * ctx,
                                ulong           idx ) {
  FD_TEST( idx<ctx->bam_work_cnt );
  FD_TEST( ctx->bam_scheduled_work_cnt<=ctx->bam_work_cnt );
  pack_tile_bam_index_remove_work( ctx, idx );
  ulong last_idx = ctx->bam_work_cnt-1UL;
  if( FD_UNLIKELY( idx>=ctx->bam_scheduled_work_cnt ) ) {
    if( FD_LIKELY( idx<last_idx ) ) pack_tile_bam_work_move( ctx, idx, last_idx );
  } else {
    /* Keep scheduled work in [0,bam_scheduled_work_cnt) and pending
       work in [bam_scheduled_work_cnt,bam_work_cnt).  Removing a
       scheduled item fills its hole from the end of the scheduled
       range, then fills the old range boundary from the pending tail. */
    ulong scheduled_last_idx = ctx->bam_scheduled_work_cnt-1UL;
    if( FD_LIKELY( idx<scheduled_last_idx ) )
      pack_tile_bam_work_move( ctx, idx, scheduled_last_idx );
    if( FD_UNLIKELY( scheduled_last_idx<last_idx ) )
      pack_tile_bam_work_move( ctx, scheduled_last_idx, last_idx );
    ctx->bam_scheduled_work_cnt--;
  }
  ctx->bam_work_cnt--;
}

static inline pack_bam_work_t *
pack_tile_bam_work_mark_scheduled( fd_pack_ctx_t * ctx,
                                   ulong           idx ) {
  FD_TEST( idx>=ctx->bam_scheduled_work_cnt && idx<ctx->bam_work_cnt );

  ulong scheduled_idx = ctx->bam_scheduled_work_cnt;
  if( FD_UNLIKELY( idx!=scheduled_idx ) ) {
    uchar mask_scheduled = ctx->bam_work[ scheduled_idx ].indexed_mask;
    uchar mask_idx       = ctx->bam_work[ idx           ].indexed_mask;
    pack_tile_bam_index_remove_work( ctx, scheduled_idx );
    pack_tile_bam_index_remove_work( ctx, idx );
    pack_bam_work_t tmp             = ctx->bam_work[ scheduled_idx ];
    ctx->bam_work[ scheduled_idx ]  = ctx->bam_work[ idx ];
    ctx->bam_work[ idx ]            = tmp;
    pack_tile_bam_index_insert_work( ctx, scheduled_idx, mask_idx       );
    pack_tile_bam_index_insert_work( ctx, idx,           mask_scheduled );
  }

  pack_bam_work_t * item = &ctx->bam_work[ scheduled_idx ];
  ctx->bam_scheduled_work_cnt++;
  return item;
}

/* Find a signature in the scheduled prefix or pending suffix.  A NULL
   matched_idx restricts the lookup to leading signatures; otherwise it
   receives the matched member index.  Return bam_work_cnt on miss.
   Check full signatures among prefix matches to resolve collisions. */
static inline ulong
pack_tile_bam_work_find( fd_pack_ctx_t const * ctx,
                          void const *          sig,
                          _Bool                 scheduled,
                          uchar *               matched_idx ) {
  ulong key = fd_ulong_load_8( sig );
  for( ulong ele_idx = pack_bam_sig_map_idx_query_const( ctx->bam_sig_map, &key, PACK_BAM_SIG_MAP_NULL, ctx->bam_sig_pool );
       ele_idx!=PACK_BAM_SIG_MAP_NULL;
       ele_idx = pack_bam_sig_map_idx_next_const( ele_idx, PACK_BAM_SIG_MAP_NULL, ctx->bam_sig_pool ) ) {
    pack_bam_sig_ele_t const * ele = &ctx->bam_sig_pool[ ele_idx ];
    if( !matched_idx && ele->txn_idx ) continue;
    if( FD_UNLIKELY( (ele->work_idx<ctx->bam_scheduled_work_cnt)!=scheduled ) ) continue;
    pack_bam_work_t const * work = &ctx->bam_work[ ele->work_idx ];
    if( FD_UNLIKELY( memcmp( work->sig[ ele->txn_idx ], sig, sizeof(fd_ed25519_sig_t) ) ) ) continue;
    if( matched_idx ) *matched_idx = ele->txn_idx;
    return ele->work_idx;
  }
  return ctx->bam_work_cnt;
}

static inline void
pack_tile_bam_work_mark_txn_scheduled( fd_pack_ctx_t *     ctx,
                                       fd_txn_e_t const * txne,
                                       long               now_ns ) {
  fd_txn_p_t const * txnp = txne->txnp;
  if( FD_LIKELY( txnp->source_tpu!=FD_TXN_M_TPU_SOURCE_BAM ) ) return;
  if( FD_UNLIKELY( txne->bam.batch_idx ) ) return;

  if( FD_UNLIKELY( !ctx->bam_first_schedule_seen ) ) {
    ctx->bam_first_schedule_seen = 1U;
    ctx->bam_first_schedule_minus_slot_end_ns = now_ns - ctx->slot_end_ns;
  }

  ulong work_idx = pack_tile_bam_work_find( ctx, fd_txn_get_signatures( TXN(txnp), txnp->payload ), 0, NULL );
  if( FD_UNLIKELY( work_idx>=ctx->bam_work_cnt ) ) return;

  pack_bam_work_t * item = pack_tile_bam_work_mark_scheduled( ctx, work_idx );
  item->max_schedule_slot = ctx->leader_slot; /* as in jito-solana, the dispatch slot */
  ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_SCHEDULED_IDX ]++;
  pack_tile_note_bam_first_outcome( ctx,
                                    FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_SCHEDULED_IDX,
                                    item->first_rx_ts_ns,
                                    now_ns );
}

static inline void
pack_tile_retire_all_pending_bam_work_by_sig( fd_pack_ctx_t * ctx,
                                              uchar const     sig[ static 64 ] ) {
  for(;;) {
    uchar matched_idx = UCHAR_MAX;
    ulong work_idx = pack_tile_bam_work_find( ctx, sig, 0, &matched_idx );
    if( FD_LIKELY( work_idx>=ctx->bam_work_cnt ) ) break;

    pack_bam_work_t const * item = &ctx->bam_work[ work_idx ];
    fd_bam_bundle_result_t res = fd_bam_result_base( item->seq_id, item->scheduler_gen, item->max_schedule_slot, item->txn_cnt );
    pack_tile_bam_work_swap_remove( ctx, work_idx );
    fd_bam_result_mark_not_committed_txn_error( &res, matched_idx, bam_types_TransactionErrorReason_ALREADY_PROCESSED );
    fd_bam_result_mark_sanitize_success_all( &res );
    pack_tile_enqueue_bam_result( ctx, &res );
  }
}

/* fd_pack accepts duplicate signatures, so the caller must check sig[0].
   Keep pending work reconciled with fd_pack: admission uses its membership
   query before consulting bam_sig_map. */

static inline int
pack_tile_append_bam_work( fd_pack_ctx_t * ctx,
                           void const *    sigs,
                           long            first_rx_ts_ns,
                           uint            seq_id,
                           ushort          scheduler_gen,
                           ulong           max_schedule_slot,
                           ulong           blockhash_height,
                           uchar           txn_cnt ) {
  if( FD_UNLIKELY( ctx->bam_work_cnt >= ctx->bam_work_max ) ) return 0;
  if( FD_UNLIKELY( ctx->bam_pending_result_cnt + pack_tile_bam_pending_work_cnt( ctx ) + 1UL >= 2UL*ctx->bam_work_max ) ) return 0;

  ulong work_idx = ctx->bam_work_cnt++;
  pack_bam_work_t * item = &ctx->bam_work[ work_idx ];
  *item = (pack_bam_work_t){
    .first_rx_ts_ns    = first_rx_ts_ns,
    .max_schedule_slot = max_schedule_slot,
    .blockhash_height  = blockhash_height,
    .seq_id            = seq_id,
    .scheduler_gen     = scheduler_gen,
    .txn_cnt           = txn_cnt,
  };
  fd_memcpy( item->sig, sigs, (ulong)txn_cnt * sizeof(fd_ed25519_sig_t) );
  pack_tile_bam_index_insert_work( ctx, work_idx, (uchar)((1U<<txn_cnt)-1U) );
  ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_PENDING_ENTERED_IDX ]++;
  return 1;
}

static inline void
pack_tile_evict_invalid_pending_bam_work( fd_pack_ctx_t * ctx,
                                          int             all ) {
  ulong old_work_cnt = ctx->bam_work_cnt;
  ulong dst          = ctx->bam_scheduled_work_cnt;
  for( ulong src=ctx->bam_scheduled_work_cnt; src<old_work_cnt; src++ ) {
    pack_bam_work_t const * work = &ctx->bam_work[ src ];
    if( FD_LIKELY( !all && !pack_tile_bam_target_expired( ctx, work->max_schedule_slot ) ) ) {
      pack_tile_bam_work_move( ctx, dst, src );
      dst++;
      continue;
    }

    /* Delete stale bundles immediately; queue results separately so
       feedback backpressure does not retain live work capacity. */
    pack_bam_work_t item = *work;
    pack_tile_bam_index_remove_work( ctx, src );
    ctx->bam_work_cnt--;
    pack_tile_log_bam_drop( ctx,
                               "post_pending_validation",
                               "pending_evicted_outside_slot",
                               1U,
                               0,
                               item.seq_id,
                               item.txn_cnt,
                               "pending",
                               item.max_schedule_slot,
                               item.blockhash_height,
                               item.first_rx_ts_ns,
                               0U,
                               0U,
                               item.txn_cnt,
                               0U,
                               item.sig[ 0 ] );

    ctx->bam_pending_work_evicted_cnt[ item.txn_cnt==1U
        ? FD_METRICS_ENUM_PACK_BAM_WORK_INVALID_REASON_V_SINGLE_OUTSIDE_SLOT_IDX
        : FD_METRICS_ENUM_PACK_BAM_WORK_INVALID_REASON_V_BUNDLE_OUTSIDE_SLOT_IDX ]++;
    ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_PENDING_EVICTED_IDX ]++;
    pack_tile_note_bam_first_outcome( ctx,
                                      FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_PENDING_EVICTED_IDX,
                                      item.first_rx_ts_ns,
                                      pack_tile_wallclock_from_ticks( ctx, fd_tickcount() ) );

    ulong deleted = fd_pack_delete_bam_bundle( ctx->pack,
                                               (fd_ed25519_sig_t const *)(void const *)&item.sig[ 0 ],
                                               item.seq_id,
                                               item.scheduler_gen );
    FD_MCNT_INC( PACK, TXN_DELETED, deleted );

    fd_bam_bundle_result_t res = pack_tile_make_bam_outside_slot_result( item.seq_id,
                                                                         item.scheduler_gen,
                                                                         item.max_schedule_slot,
                                                                         item.txn_cnt );
    pack_tile_enqueue_bam_result( ctx, &res );
  }
  FD_TEST( ctx->bam_work_cnt==dst );
}

/* Readiness belongs to this exact candidate, never to a traversal filter.
   Return 1 if ready, 0 if held, and -1 if normal missed-target eviction
   invalidated the candidate.  A caller preparing an initializer must
   refresh the view after -1 before inspecting the successor's metadata. */
static inline int
pack_tile_bam_candidate_ready( fd_pack_ctx_t *       ctx,
                               fd_txn_e_t const *    candidate ) {
  if( FD_UNLIKELY( !candidate || ctx->leader_slot==ULONG_MAX || ctx->drain_execle ) ) return 0;
  fd_txn_p_t const * txnp = candidate->txnp;
  if( FD_UNLIKELY( txnp->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE ) ) {
    if( FD_LIKELY( !ctx->bam_override_snapshot ) ) return 1;
    return ctx->bam_ib_associated && ctx->leader_slot>=ctx->bam_min_admission_slot &&
           !memcmp( fd_txn_get_signatures( TXN(txnp), txnp->payload ), ctx->crank->last_sig, sizeof(fd_ed25519_sig_t) );
  }
  if( FD_LIKELY( txnp->source_tpu!=FD_TXN_M_TPU_SOURCE_BAM ) ) return 1;
  /* Normal-mode traversal can see BAM entries while ownership changes.
     They must not dispatch or prepare a normal-mode fee initializer. */
  if( FD_UNLIKELY( !ctx->bam_override_snapshot ) ) return 0;
  ulong idx = pack_tile_bam_work_find( ctx,
                  fd_txn_get_signatures( TXN(txnp), txnp->payload ), 0, NULL );
  if( FD_UNLIKELY( idx>=ctx->bam_work_cnt || candidate->bam.batch_idx ||
                   ctx->bam_work[ idx ].seq_id!=candidate->bam.seq_id ||
                   ctx->bam_work[ idx ].scheduler_gen!=candidate->bam.scheduler_gen ) ) {
    /* Diagnose once, then fail closed without logging on every attempt. */
    if( FD_UNLIKELY( !ctx->bam_candidate_identity_mismatch_cnt++ ) )
      FD_LOG_WARNING(( "BAM candidate has no matching pending work identity" ));
    return 0;
  }
  ulong target = ctx->bam_work[ idx ].max_schedule_slot;
  if( FD_UNLIKELY( pack_tile_bam_target_expired( ctx, target ) ) ) {
    pack_tile_evict_invalid_pending_bam_work( ctx, 0 );
    return -1;
  }
  return target>=ctx->leader_slot && ctx->leader_slot>=ctx->bam_min_admission_slot;
}

/* Lookahead cannot mutate Pack while checking predecessors.  A held,
   incomplete or foreign batch remains a barrier; readiness is certified
   separately for every complete batch in the bounded prefix. */
static int
pack_tile_bam_lookahead_ready( void const *         _ctx,
                               fd_txn_e_t const *   candidate,
                               ulong                txn_cnt ) {
  fd_pack_ctx_t const * ctx = _ctx;
  if( FD_UNLIKELY( !ctx->bam_override_snapshot || ctx->leader_slot==ULONG_MAX || ctx->drain_execle ||
                   ctx->leader_slot<ctx->bam_min_admission_slot ) ) return 0;
  ulong idx = pack_tile_bam_work_find( ctx,
                  fd_txn_get_signatures( TXN(candidate->txnp), candidate->txnp->payload ), 0, NULL );
  if( FD_UNLIKELY( idx>=ctx->bam_work_cnt ) ) return 0;
  pack_bam_work_t const * work = &ctx->bam_work[idx];
  return work->max_schedule_slot>=ctx->leader_slot &&
         work->seq_id==candidate->bam.seq_id && work->scheduler_gen==candidate->bam.scheduler_gen &&
         work->txn_cnt==txn_cnt && work->indexed_mask==(uchar)((1U<<txn_cnt)-1U);
}

/* after_credit's schedule call with BAM configured: the candidate hint
   and lookahead readiness.  Without BAM, after_credit calls the unhinted
   fd_pack_schedule_next_microblock, as upstream. */
static inline ulong
pack_tile_bam_schedule( fd_pack_ctx_t * ctx,
                        int             execle_idx,
                        int             flags,
                        ulong           bundle_hint,
                        fd_txn_e_t *    out ) {
  return fd_pack_schedule_next_microblock_with_bundle_hint( ctx->pack, CUS_PER_MICROBLOCK, VOTE_FRACTION, (ulong)execle_idx,
                                                            flags, bundle_hint, pack_tile_bam_lookahead_ready, ctx, out );
}

/* fd_pack reports how many bundles it evicted, not which ones. Retire
   missing pending records so admission's membership check cannot miss
   a duplicate. Scan when fd_pack_bundle_evicted_cnt changes, or after
   explicit signature-wide deletion in remove_ib.

   The counter excludes expiry, which never removes BAM work (BAM
   expires_at is ULONG_MAX), and clear_all, which is only used by tests. */

static inline void
pack_tile_reconcile_pending_bam_work( fd_pack_ctx_t * ctx ) {
  long  now_ns       = pack_tile_wallclock_from_ticks( ctx, fd_tickcount() );
  ulong old_work_cnt = ctx->bam_work_cnt;
  ulong dst          = ctx->bam_scheduled_work_cnt;
  for( ulong src=ctx->bam_scheduled_work_cnt; src<old_work_cnt; src++ ) {
    pack_bam_work_t const * work = &ctx->bam_work[ src ];
    if( FD_LIKELY( fd_pack_contains_bam_bundle( ctx->pack,
                                               (fd_ed25519_sig_t const *)(void const *)&work->sig[ 0 ],
                                               work->seq_id,
                                               work->scheduler_gen,
                                               1 ) ) ) {
      pack_tile_bam_work_move( ctx, dst, src );
      dst++;
      continue;
    }

    pack_bam_work_t item = *work;
    pack_tile_bam_index_remove_work( ctx, src );
    ctx->bam_work_cnt--;
    pack_tile_log_bam_drop( ctx,
                               "post_pending_validation",
                               "pending_evicted_by_pack",
                               0U,
                               0,
                               item.seq_id,
                               item.txn_cnt,
                               "pending",
                               item.max_schedule_slot,
                               item.blockhash_height,
                               item.first_rx_ts_ns,
                               0U,
                               0U,
                               item.txn_cnt,
                               0U,
                               item.sig[ 0 ] );

    ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_PENDING_EVICTED_IDX ]++;
    pack_tile_note_bam_first_outcome( ctx,
                                      FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_PENDING_EVICTED_IDX,
                                      item.first_rx_ts_ns,
                                      now_ns );

    /* Pack dropped it to make room, so report it the same way an insert
       that lost the priority contest is reported. */
    fd_bam_bundle_result_t res = fd_bam_result_base( item.seq_id,
                                                     item.scheduler_gen,
                                                     item.max_schedule_slot,
                                                     item.txn_cnt );
    res.scheduling_error = FD_BAM_SCHED_ERR_CONTAINER_FULL;
    pack_tile_enqueue_bam_result( ctx, &res );
  }
  FD_TEST( ctx->bam_work_cnt==dst );
}

/* Runs pack_tile_reconcile_pending_bam_work only when pack has evicted a
   bundle since the last check.  Call this after any insertion that reports
   deletions, before later code relies on pack's signature map indexing the
   pending suffix. */

static inline void
pack_tile_maybe_reconcile_pending_bam_work( fd_pack_ctx_t * ctx ) {
  ulong bundle_evicted_cnt = fd_pack_bundle_evicted_cnt( ctx->pack );
  if( FD_UNLIKELY( bundle_evicted_cnt!=ctx->bam_pack_bundle_evicted_cnt ) ) {
    ctx->bam_pack_bundle_evicted_cnt = bundle_evicted_cnt;
    if( FD_UNLIKELY( pack_tile_bam_pending_work_cnt( ctx ) ) ) pack_tile_reconcile_pending_bam_work( ctx );
  }
}

static inline int
pack_tile_drain_one_pending_bam_result( fd_pack_ctx_t *     ctx,
                                        fd_stem_context_t * stem ) {
  if( FD_LIKELY( !ctx->bam_pending_result_cnt ) ) return 0;

  fd_bam_bundle_result_t const * res = &ctx->bam_result_queue[ ctx->bam_result_queue_head ];
  fd_bam_publish_result( stem,
                         ctx->bam_result_out.out_idx,
                         ctx->bam_result_out.mem,
                         &ctx->bam_result_out.chunk,
                         ctx->bam_result_out.chunk0,
                         ctx->bam_result_out.wmark,
                         res );
  /* Advance by one with a compare instead of a modulo; this runs once per
     drained result. */
  ctx->bam_result_queue_head++;
  if( FD_UNLIKELY( ctx->bam_result_queue_head>=2UL*ctx->bam_work_max ) ) ctx->bam_result_queue_head = 0UL;
  ctx->bam_pending_result_cnt--;
  return 1;
}

static inline void
pack_tile_publish_bam_insert_reject( fd_pack_ctx_t *     ctx,
                                     uint                seq_id,
                                     ushort              scheduler_gen,
                                     ulong               max_schedule_slot,
                                     uchar               txn_cnt,
                                     ulong               reject_txn_idx,
                                     int                 pack_rc ) {
  fd_bam_bundle_result_t res = fd_bam_result_base( seq_id, scheduler_gen, max_schedule_slot, txn_cnt );
  if( FD_UNLIKELY( pack_rc==FD_PACK_INSERT_REJECT_PRIORITY || pack_rc==FD_PACK_INSERT_REJECT_DUPLICATE ) ) {
    res.scheduling_error = FD_BAM_SCHED_ERR_CONTAINER_FULL;
  } else if( FD_UNLIKELY( pack_rc==FD_PACK_INSERT_REJECT_INSTR_ACCT_CNT ) ) {
    res.bundle_err   = FD_BAM_BUNDLE_ERR_DESER;
    res.deser_index  = (uchar)reject_txn_idx;
    res.deser_reason = bam_types_DeserializationErrorReason_SANITIZE_ERROR;
  } else {
    fd_bam_result_mark_not_committed_txn_error( &res, reject_txn_idx, fd_bam_txn_err_from_pack_insert( pack_rc ) );
    fd_bam_result_mark_sanitize_success_all( &res );
  }
  pack_tile_enqueue_bam_result( ctx, &res );
}

static inline void
pack_tile_publish_bam_tracking_reject( fd_pack_ctx_t *          ctx,
                                       void const *             sig0,
                                       long                     first_rx_ts_ns,
                                       uint                     seq_id,
                                       ushort                   scheduler_gen,
                                       ulong                    max_schedule_slot,
                                       ulong                    blockhash_height,
                                       uchar                    txn_cnt ) {
  ulong deleted = fd_pack_delete_bam_bundle( ctx->pack,
                                             (fd_ed25519_sig_t const *)sig0,
                                             seq_id,
                                             scheduler_gen );
  FD_MCNT_INC( PACK, TXN_DELETED, deleted );
  pack_tile_log_bam_drop( ctx,
                             "tracking",
                             "tracking_rejected",
                             0U,
                             0,
                             seq_id,
                             txn_cnt,
                             "staging",
                             max_schedule_slot,
                             blockhash_height,
                             first_rx_ts_ns,
                             1U,
                             1U,
                             txn_cnt,
                             0U,
                             sig0 );
  ctx->bam_tracking_rejected_cnt++;
  ctx->bam_tracking_rejected_txn_cnt += txn_cnt;
  pack_tile_note_bam_first_outcome( ctx,
                                    FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_TRACKING_REJECTED_IDX,
                                    first_rx_ts_ns,
                                    pack_tile_wallclock_from_ticks( ctx, fd_tickcount() ) );
  ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_REJECTED_PRE_PENDING_IDX ]++;

  fd_bam_bundle_result_t res = fd_bam_result_base( seq_id, scheduler_gen, max_schedule_slot, txn_cnt );
  res.scheduling_error = FD_BAM_SCHED_ERR_CONTAINER_FULL;
  pack_tile_enqueue_bam_result( ctx, &res );
}

static inline void
pack_tile_refresh_bam_fee_meta( fd_pack_ctx_t * ctx ) {
  fd_bam_fee_cfg_t const * cfg = ctx->bam_fee_cfg;
  if( FD_UNLIKELY( !cfg ) ) return;

  for( int attempt=0; attempt<4; attempt++ ) {
    uint v0 = FD_VOLATILE_CONST( cfg->version );
    if( FD_UNLIKELY( !v0 || fd_uint_extract_bit( v0, 31 ) ) ) continue;
    if( FD_LIKELY( v0==ctx->bam_fee_cfg_version ) ) return;

    uchar builder_pubkey[ 32 ];
    FD_HW_MFENCE_LD();
    uint builder_commission = FD_VOLATILE_CONST( cfg->builder_commission );
    fd_memcpy( builder_pubkey, cfg->builder_pubkey, sizeof(builder_pubkey) );
    FD_HW_MFENCE_LD();
    if( FD_UNLIKELY( FD_VOLATILE_CONST( cfg->version )!=v0 ) ) continue;

    fd_memcpy( ctx->bam_fee_meta->commission_pubkey->b, builder_pubkey, sizeof(builder_pubkey) );
    ctx->bam_fee_meta->commission = (ulong)builder_commission;
    ctx->bam_fee_cfg_version = v0;
    return;
  }
}

static inline void
pack_tile_abandon_current_bam_bundle( fd_pack_ctx_t *              ctx,
                                      pack_tile_bam_bundle_assembly_abandon_reason_t reason ) {
  if( FD_UNLIKELY( !ctx->current_bundle_bam->is_bam ||
                   ( !ctx->current_bundle->bundle &&
                     reason!=PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_NEW_SEQ_BEFORE_COMPLETE ) ) ) return;
  ctx->bam_bundle_assembly_abandon_cnt[ (ulong)reason ]++;
  long first_rx_ts_ns = ctx->current_bundle->bundle
                         ? pack_tile_current_bam_bundle_first_rx_ts_ns( ctx )
                         : 0L;
  pack_tile_note_bam_first_outcome( ctx,
                                    FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_BUNDLE_ASSEMBLY_ABANDONED_IDX,
                                    first_rx_ts_ns,
                                    pack_tile_wallclock_from_ticks( ctx, fd_tickcount() ) );
  fd_ed25519_sig_t sig0[1] = {{0}};
  _Bool have_sig0 = !!ctx->current_bundle->bundle && !!ctx->current_bundle->txn_received;
  if( FD_LIKELY( have_sig0 ) ) {
    fd_txn_p_t const * txnp = ctx->current_bundle->_txn[ 0 ]->txnp;
    fd_memcpy( sig0, fd_txn_get_signatures( TXN(txnp), txnp->payload ), sizeof(fd_ed25519_sig_t) );
  }

  uint  first_missing_idx = (uint)ctx->current_bundle->txn_received;
  _Bool is_new_seq_abandon = reason==PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_NEW_SEQ_BEFORE_COMPLETE;
  uint seq_id = (uint)( ctx->current_bundle->id - 1UL );
  pack_tile_log_bam_drop( ctx,
                             "bundle_assembly",
                             is_new_seq_abandon                                             ? "bundle_assembly_abandon_new_seq_before_complete" :
                             reason==PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_LEADER_SLOT_END ? "bundle_assembly_abandon_leader_slot_end" :
                                                                                              "bundle_assembly_abandon_poh_timeout",
                             0U,
                             0,
                             seq_id,
                             (uchar)ctx->current_bundle->txn_cnt,
                             "assembling",
                             ctx->current_bundle_bam->max_schedule_slot,
                             ctx->current_bundle->min_blockhash_height,
                             first_rx_ts_ns,
                             1U,
                             1U,
                             ctx->current_bundle->txn_received,
                             1U,
                             have_sig0 ? sig0 : NULL );

  if( FD_LIKELY( is_new_seq_abandon ) ) {
    fd_bam_bundle_result_t res = fd_bam_result_base( seq_id,
                                                     ctx->current_bundle_bam->scheduler_gen,
                                                     ctx->current_bundle_bam->max_schedule_slot,
                                                     (uchar)ctx->current_bundle->txn_cnt );
    res.bundle_err   = FD_BAM_BUNDLE_ERR_DESER;
    res.deser_index  = (uchar)first_missing_idx;
    res.deser_reason = bam_types_DeserializationErrorReason_SANITIZE_ERROR;
    pack_tile_enqueue_bam_result( ctx, &res );
  }

  if( FD_LIKELY( ctx->current_bundle->bundle ) )
    fd_pack_insert_bundle_cancel( ctx->pack, ctx->current_bundle->bundle, ctx->current_bundle->txn_cnt );
  ctx->current_bundle->bundle = NULL;
  /* At slot end, retain only the batch identity and received count.
     The remaining ordered members decide SANITIZE versus OUTSIDE. */
  ctx->current_bundle_bam->is_bam = (uchar)!is_new_seq_abandon;
}

/* Retire partial and pending work before acknowledging the new ownership
   generation.  Already-dispatched work remains tracked through completion. */
static __attribute__((noinline)) void
pack_tile_sync_bam_ownership_generation_slow( fd_pack_ctx_t * ctx,
                                              ulong           gen_state ) {
  ushort requested_gen = (ushort)(gen_state>>1);

  if( FD_UNLIKELY( requested_gen!=ctx->bam_ownership_gen ) ) {
    if( FD_UNLIKELY( ctx->bam_ib_associated ) ) {
      /* A queued initializer is bundle-source work outside bam_work.
         Do not delete an initializer that already dispatched. */
      if( FD_UNLIKELY( ctx->crank->ib_inserted &&
                       fd_pack_contains_initializer_bundle( ctx->pack,
                           (fd_ed25519_sig_t const *)ctx->crank->last_sig, 1 ) ) ) remove_ib( ctx );
      ctx->bam_ib_associated = 0;
    }
    if( FD_UNLIKELY( ctx->current_bundle_bam->is_bam &&
                     ctx->current_bundle_bam->ownership_gen!=requested_gen ) ) {
      if( FD_LIKELY( ctx->current_bundle->bundle ) )
        fd_pack_insert_bundle_cancel( ctx->pack,
                                      ctx->current_bundle->bundle,
                                      ctx->current_bundle->txn_cnt );
      ctx->current_bundle->bundle       = NULL;
      ctx->current_bundle->txn_received = 0UL;
      ctx->current_bundle_bam->is_bam   = 0U;
      ctx->bundle_kind                  = PACK_TILE_BUNDLE_KIND_NONE;
      ctx->cur_spot                     = NULL;
    }

    while( pack_tile_bam_pending_work_cnt( ctx ) ) {
      ulong work_idx = ctx->bam_scheduled_work_cnt;
      pack_bam_work_t const * work = &ctx->bam_work[ work_idx ];
      ulong deleted = fd_pack_delete_bam_bundle(
          ctx->pack,
          (fd_ed25519_sig_t const *)(void const *)&work->sig[ 0 ],
          work->seq_id,
          work->scheduler_gen );
      FD_MCNT_INC( PACK, TXN_DELETED, deleted );
      pack_tile_bam_work_swap_remove( ctx, work_idx );
    }
    ctx->bam_ownership_gen = requested_gen;
  }

  if( FD_UNLIKELY( gen_state & 1UL ) ) {
    FD_HW_MFENCE_ST();
    (void)FD_ATOMIC_CAS( ctx->bam_gen_fseq, gen_state, gen_state & ~1UL );
  }
}

/* Called several times per BAM transaction: inline only the test for an
   unchanged, acknowledged generation. */
static inline void
pack_tile_sync_bam_ownership_generation( fd_pack_ctx_t * ctx ) {
  if( FD_UNLIKELY( !ctx->bam_gen_fseq ) ) return;
  ulong gen_state = fd_fseq_query( ctx->bam_gen_fseq );
  if( FD_LIKELY( (ushort)(gen_state>>1)==ctx->bam_ownership_gen && !(gen_state & 1UL) ) ) return;
  pack_tile_sync_bam_ownership_generation_slow( ctx, gen_state );
}

/* BAM part of ending a leader slot, called at the start of each
   end-of-slot sequence while leader_slot is still the closing slot. */
static inline void
pack_tile_bam_end_slot( fd_pack_ctx_t * ctx,
                        long            now,
                        char const *    reason,
                        pack_tile_bam_bundle_assembly_abandon_reason_t bam_abandon_reason ) {
  if( FD_LIKELY( !ctx->bam_status_fseq ) ) return; /* BAM not configured */
  if( FD_UNLIKELY( ctx->dump_bam_mode ) ) {
    long observed_ns = pack_tile_wallclock_from_ticks( ctx, now );
    FD_LOG_NOTICE(( "Firedancer slot end: pack_current_slot=%lu slot_end_ns=%ld observed_ns=%ld observed_minus_slot_end_ns=%ld reason=%s current_slot_has_bam_work=%u", ctx->leader_slot, ctx->slot_end_ns, observed_ns, observed_ns - ctx->slot_end_ns, reason, (uint)ctx->bam_current_slot_has_bam_work ));
  }

  ulong first_insert_result_idx = pack_tile_bam_first_event_result_idx( ctx->bam_first_insert_seen,
                                                                         ctx->bam_first_insert_minus_slot_end_ns );
  ulong first_schedule_result_idx = pack_tile_bam_first_event_result_idx( ctx->bam_first_schedule_seen,
                                                                           ctx->bam_first_schedule_minus_slot_end_ns );
  ctx->bam_first_insert_result_cnt[ first_insert_result_idx ]++;
  ctx->bam_first_schedule_result_cnt[ first_schedule_result_idx ]++;
  /* As in jito-solana, a slot change retires all pending work.  The
     closed-slot floor rejects later arrivals for this slot. */
  ulong work_cnt = ctx->bam_work_cnt;
  ctx->bam_min_admission_slot = fd_ulong_max( ctx->bam_min_admission_slot, fd_ulong_sat_add( ctx->leader_slot, 1UL ) );
  pack_tile_evict_invalid_pending_bam_work( ctx, 1 );
  FD_MCNT_INC( PACK, BAM_UNSCHEDULED_AT_SLOT_END, work_cnt-ctx->bam_work_cnt );

  /* Cancel any BAM bundle assembly that never reached a publishable
     result.  A Block Engine bundle keeps assembling, as upstream. */
  pack_tile_abandon_current_bam_bundle( ctx, bam_abandon_reason );

  ctx->bam_current_slot_has_bam_work = 0U;
  ctx->bam_first_insert_seen         = 0U;
  ctx->bam_first_schedule_seen       = 0U;
}

static inline _Bool
pack_tile_bam_override_active( fd_pack_ctx_t const * ctx ) {
  return !!ctx->bam_status_fseq && !!( fd_fseq_query( ctx->bam_status_fseq ) & FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
}

/* Observe the mode once for initializer selection and the subsequent
   scheduling call.  A pending initializer from the prior mode must not
   survive the edge and become eligible under the new mode.  As in
   jito-solana, BAM activation clears pending Block Engine bundles. */
static inline _Bool
pack_tile_snapshot_bam_override( fd_pack_ctx_t * ctx ) {
  _Bool bam_override = pack_tile_bam_override_active( ctx );
  if( FD_UNLIKELY( bam_override!=ctx->bam_override_snapshot ) ) {
    remove_ib( ctx );
    if( bam_override ) FD_MCNT_INC( PACK, TXN_DELETED, fd_pack_delete_non_bam_bundles( ctx->pack ) );
    ctx->bam_override_snapshot = bam_override;
  }
  return bam_override;
}

static int pack_tile_bam_batch_after_frag( fd_pack_ctx_t * ctx, fd_stem_context_t * stem );

/* pack_tile_bam_resolv_after_frag handles the resolv frags FireBAM owns:
   BAM batches, and Block Engine bundles while BAM controls the TPU.  As
   in jito-solana, TPU transactions are buffered in either mode; BAM mode
   only stops scheduling them (FD_PACK_SCHEDULE_BAM_ONLY).  Returns 1 if
   the frag was consumed. */
static inline int
pack_tile_bam_resolv_after_frag( fd_pack_ctx_t *     ctx,
                                 fd_stem_context_t * stem ) {
  if( FD_LIKELY( ctx->bundle_kind==PACK_TILE_BUNDLE_KIND_NONE ) ) return !ctx->cur_spot;
  if( FD_LIKELY( ctx->bundle_kind==PACK_TILE_BUNDLE_KIND_BLOCK_ENGINE ) ) {
    if( FD_LIKELY( !pack_tile_bam_override_active( ctx ) ) ) return 0;
    if( FD_LIKELY( ctx->current_bundle->bundle ) ) {
      FD_MCNT_INC( PACK, TXN_PARTIAL_BUNDLE,
                   fd_ulong_min( ctx->current_bundle->txn_received+1UL, ctx->current_bundle->txn_cnt ) );
      fd_pack_insert_bundle_cancel( ctx->pack, ctx->current_bundle->bundle, ctx->current_bundle->txn_cnt );
      ctx->current_bundle->bundle = NULL;
    }
    ctx->current_bundle->txn_received = 0UL;
    ctx->bundle_kind = PACK_TILE_BUNDLE_KIND_NONE;
    return 1;
  }
  return pack_tile_bam_batch_after_frag( ctx, stem );
}

/* The BAM batch member case of pack_tile_bam_resolv_after_frag, out of
   line so that its large frame is not set up for every TPU frag. */
static __attribute__((noinline)) int
pack_tile_bam_batch_after_frag( fd_pack_ctx_t *     ctx,
                                fd_stem_context_t * stem ) {
  if( FD_UNLIKELY( !ctx->cur_spot ) ) {
    ctx->current_bundle->txn_received++;
    if( FD_LIKELY( ctx->current_bundle->txn_received==ctx->current_bundle->txn_cnt ) ) {
      fd_bam_bundle_result_t res = pack_tile_make_bam_outside_slot_result( (uint)(ctx->current_bundle->id-1UL),
                                                                           ctx->current_bundle_bam->scheduler_gen,
                                                                           ctx->current_bundle_bam->max_schedule_slot,
                                                                           (uchar)ctx->current_bundle->txn_cnt );
      pack_tile_enqueue_bam_result( ctx, &res );
      ctx->current_bundle_bam->is_bam = 0;
    }
    return 1;
  }

  FD_TEST( ctx->cur_spot->bam.batch_idx==ctx->current_bundle->txn_received );
  ctx->current_bundle->txn_received++;

  if( FD_UNLIKELY( ctx->current_bundle->txn_received!=ctx->current_bundle->txn_cnt ) ) return 1;

  uint seq_id                = (uint)( ctx->current_bundle->id - 1UL );
  uchar txn_cnt              = (uchar)ctx->current_bundle->txn_cnt;
  ulong max_schedule_slot    = ctx->current_bundle_bam->max_schedule_slot;
  ulong min_blockhash_height = ctx->current_bundle->min_blockhash_height;
  long first_rx_ts_ns        = pack_tile_current_bam_bundle_first_rx_ts_ns( ctx );
  if( FD_UNLIKELY( pack_tile_bam_target_expired( ctx, max_schedule_slot ) ) ) {
    pack_tile_log_bam_drop( ctx,
                            "pre_pending_validation",
                            "rejected_pre_pending_outside_slot",
                            1U,
                            0,
                            seq_id,
                            txn_cnt,
                            "assembling",
                            max_schedule_slot,
                            min_blockhash_height,
                            first_rx_ts_ns,
                            1U,
                            1U,
                            txn_cnt,
                            0U,
                            fd_txn_get_signatures( TXN(ctx->current_bundle->bundle[ 0 ]->txnp),
                            ctx->current_bundle->bundle[ 0 ]->txnp->payload ) );
    ctx->bam_work_rejected_pre_pending_cnt[ txn_cnt==1U
      ? FD_METRICS_ENUM_PACK_BAM_WORK_INVALID_REASON_V_SINGLE_OUTSIDE_SLOT_IDX
      : FD_METRICS_ENUM_PACK_BAM_WORK_INVALID_REASON_V_BUNDLE_OUTSIDE_SLOT_IDX ]++;
    pack_tile_note_bam_first_outcome( ctx,
                                      FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_REJECTED_PRE_PENDING_IDX,
                                      first_rx_ts_ns,
                                      pack_tile_wallclock_from_ticks( ctx, fd_tickcount() ) );
    ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_REJECTED_PRE_PENDING_IDX ]++;
    fd_bam_bundle_result_t res = pack_tile_make_bam_outside_slot_result( seq_id,
                                                                         ctx->current_bundle_bam->scheduler_gen,
                                                                         max_schedule_slot,
                                                                         txn_cnt );
    pack_tile_enqueue_bam_result( ctx, &res );
    fd_pack_insert_bundle_cancel( ctx->pack, ctx->current_bundle->bundle, ctx->current_bundle->txn_cnt );
    ctx->current_bundle->bundle = NULL;
    ctx->current_bundle_bam->is_bam = 0;
    return 1;
  }

  pack_tile_refresh_bam_fee_meta( ctx );
  *ctx->blk_engine_cfg = *ctx->bam_fee_meta;
  ctx->blk_engine_cfg->is_bam = 1;

  fd_ed25519_sig_t bam_sig[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  for( uchar i=0U; i<FD_PACK_MAX_TXN_PER_BUNDLE; i++ ) {
    if( FD_UNLIKELY( i>=txn_cnt ) ) break;
    fd_txn_p_t const * txnp = ctx->current_bundle->bundle[ i ]->txnp;
    fd_memcpy( &bam_sig[ i ],
               fd_txn_get_signatures( TXN(txnp), txnp->payload ),
               sizeof(fd_ed25519_sig_t) );
  }

  int pre_insert_duplicate_reject = 0;
  /* Check scheduled work directly. For pending work, first confirm
     sig0 leads a BAM bundle in fd_pack; unrelated transactions and
     non-leading members must not count as duplicate batches. */
  ulong duplicate_work_idx = pack_tile_bam_work_find( ctx, bam_sig[ 0 ], 1, NULL );
  if( FD_LIKELY( duplicate_work_idx>=ctx->bam_work_cnt ) &&
      FD_UNLIKELY( fd_pack_contains_bam_bundle( ctx->pack,
                                                (fd_ed25519_sig_t const *)(void const *)bam_sig[ 0 ],
                                                0U,
                                                0U,
                                                0 ) ) ) {
    duplicate_work_idx = pack_tile_bam_work_find( ctx, bam_sig[ 0 ], 0, NULL );
  }
  if( FD_UNLIKELY( duplicate_work_idx<ctx->bam_work_cnt ) ) {
    pack_bam_work_t * duplicate = &ctx->bam_work[ duplicate_work_idx ];
    /* Same-sequence resends may replace pending work. Cross-sequence
       duplicates must leave the old sequence tracked to preserve its
       eventual durable result. */
    if( FD_LIKELY( duplicate_work_idx>=ctx->bam_scheduled_work_cnt &&
                   duplicate->seq_id==seq_id &&
                   duplicate->scheduler_gen==ctx->current_bundle_bam->scheduler_gen &&
                   duplicate->max_schedule_slot==max_schedule_slot ) ) {
      uint   duplicate_seq_id        = duplicate->seq_id;
      ushort duplicate_scheduler_gen = duplicate->scheduler_gen;
      pack_tile_bam_work_swap_remove( ctx, duplicate_work_idx );
      ulong duplicate_deleted = fd_pack_delete_bam_bundle( ctx->pack,
                                                           (fd_ed25519_sig_t const *)(void const *)bam_sig[ 0 ],
                                                           duplicate_seq_id,
                                                           duplicate_scheduler_gen );
      FD_MCNT_INC( PACK, TXN_DELETED, duplicate_deleted );
    } else {
      pre_insert_duplicate_reject = 1;
    }
  }

  int   result         = FD_PACK_INSERT_REJECT_DUPLICATE;
  ulong reject_txn_idx = 0UL;
  if( FD_UNLIKELY( pre_insert_duplicate_reject ) ) {
    fd_pack_insert_bundle_cancel( ctx->pack, ctx->current_bundle->bundle, ctx->current_bundle->txn_cnt );
  } else {
    ulong deleted;
    long insert_duration = -fd_tickcount();
    result = fd_pack_insert_bundle_fini( ctx->pack,
                                         ctx->current_bundle->bundle,
                                         ctx->current_bundle->txn_cnt,
                                         min_blockhash_height,
                                         0,
                                         ctx->blk_engine_cfg,
                                         &deleted );
    insert_duration += fd_tickcount();
    reject_txn_idx   = fd_pack_insert_bundle_reject_txn_idx( ctx->pack );
    if( FD_UNLIKELY( deleted ) ) pack_tile_maybe_reconcile_pending_bam_work( ctx );

    FD_MCNT_INC( PACK, TXN_DELETED, deleted );
    ctx->insert_result[ result + FD_PACK_INSERT_RETVAL_OFF ] += ctx->current_bundle->txn_received;
    fd_histf_sample( ctx->insert_duration, (ulong)insert_duration );
  }

  ctx->current_bundle->bundle = NULL;
  ctx->current_bundle_bam->is_bam = 0;
  if( FD_UNLIKELY( result<0 ) ) {
    pack_tile_log_bam_drop( ctx,
                            pre_insert_duplicate_reject ? "pre_insert_duplicate" : "insert",
                            pack_tile_bam_pack_insert_reason_cstr( result ),
                            0U,
                            result,
                            seq_id,
                            txn_cnt,
                            "assembling",
                            max_schedule_slot,
                            min_blockhash_height,
                            first_rx_ts_ns,
                            1U,
                            1U,
                            txn_cnt,
                            0U,
                            bam_sig[ 0 ] );
    pack_tile_note_bam_first_outcome( ctx,
                                      FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_REJECTED_PRE_PENDING_IDX,
                                      first_rx_ts_ns,
                                      pack_tile_wallclock_from_ticks( ctx, fd_tickcount() ) );
    ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_REJECTED_PRE_PENDING_IDX ]++;
    pack_tile_publish_bam_insert_reject( ctx,
                                         seq_id,
                                         ctx->current_bundle_bam->scheduler_gen,
                                         max_schedule_slot,
                                         txn_cnt,
                                         reject_txn_idx,
                                         result );
    return 1;
  }
  if( FD_UNLIKELY( !pack_tile_append_bam_work( ctx,
                                               bam_sig[ 0 ],
                                               first_rx_ts_ns,
                                               seq_id,
                                               ctx->current_bundle_bam->scheduler_gen,
                                               max_schedule_slot,
                                               min_blockhash_height,
                                               txn_cnt ) ) ) {
    pack_tile_publish_bam_tracking_reject( ctx,
                                           bam_sig[ 0 ],
                                           first_rx_ts_ns,
                                           seq_id,
                                           ctx->current_bundle_bam->scheduler_gen,
                                           max_schedule_slot,
                                           min_blockhash_height,
                                           txn_cnt );
    return 1;
  }
  ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_ACCEPTED_IDX ]++;
  pack_tile_note_first_bam_insert( ctx,
                                   stem,
                                   pack_tile_wallclock_from_ticks( ctx, fd_tickcount() ),
                                   max_schedule_slot );
  return 1;
}

static inline void
pack_tile_bam_metrics_write( fd_pack_ctx_t * ctx ) {
  FD_MCNT_ENUM_COPY( PACK, BAM_BUNDLE_ASSEMBLY_ABANDON,   ctx->bam_bundle_assembly_abandon_cnt );
  FD_MCNT_ENUM_COPY( PACK, BAM_WORK_REJECTED_PRE_PENDING, ctx->bam_work_rejected_pre_pending_cnt );
  FD_MCNT_ENUM_COPY( PACK, BAM_PENDING_WORK_EVICTED,      ctx->bam_pending_work_evicted_cnt );
  FD_MCNT_ENUM_COPY( PACK, BAM_WORK_ITEMS,                ctx->bam_work_item_stage_cnt );
  FD_MCNT_ENUM_COPY( PACK, BAM_WORK_FIRST_OUTCOME,        ctx->bam_work_first_outcome_cnt );
  FD_MCNT_SET(      PACK, BAM_TRACKING_REJECTED,          ctx->bam_tracking_rejected_cnt );
  FD_MCNT_SET(      PACK, BAM_TRACKING_REJECTED_TRANSACTIONS, ctx->bam_tracking_rejected_txn_cnt );
  FD_MGAUGE_SET(     PACK, BAM_PENDING_WORK_COUNT,        pack_tile_bam_pending_work_cnt( ctx ) );
  FD_MGAUGE_SET(     PACK, LEADER_SLOT,                   ctx->leader_slot==ULONG_MAX ? 0UL : ctx->leader_slot );
  FD_MGAUGE_SET(     PACK, LEADER_SLOT_END_NANOS,         ctx->leader_slot==ULONG_MAX ? 0UL : (ulong)ctx->slot_end_ns );
  FD_MCNT_ENUM_COPY( PACK, BAM_LEADER_SLOT_FIRST_INSERT_RESULT,   ctx->bam_first_insert_result_cnt );
  FD_MCNT_ENUM_COPY( PACK, BAM_LEADER_SLOT_FIRST_SCHEDULE_RESULT, ctx->bam_first_schedule_result_cnt );
  FD_MHIST_COPY(     PACK, BAM_WORK_RX_TO_FIRST_OUTCOME_NANOS,              ctx->bam_work_rx_to_first_outcome_nanos );
}

/* pack_tile_bam_admit_frag syncs the BAM ownership generation before a
   BAM frag is used.  Returns 0 if the frag belongs to a retired
   generation and is dropped. */
static inline int
pack_tile_bam_admit_frag( fd_pack_ctx_t *    ctx,
                          fd_txn_m_t const * txnm ) {
  pack_tile_sync_bam_ownership_generation( ctx );
  if( FD_UNLIKELY( ctx->bam_gen_fseq &&
                   txnm->bam.ownership_gen!=ctx->bam_ownership_gen ) ) {
    ctx->bundle_kind = PACK_TILE_BUNDLE_KIND_NONE;
    return 0;
  }
  FD_TEST( txnm->bam.txn_cnt>0U && txnm->bam.txn_cnt<=FD_PACK_MAX_TXN_PER_BUNDLE );
  FD_TEST( txnm->bam.batch_idx<txnm->bam.txn_cnt );
  return 1;
}

/* pack_tile_bam_preprocess_failed answers a BAM batch member that failed
   ingress preprocessing with a SANITIZE deserialization error, after
   abandoning or cancelling the batch being assembled. */
static inline void
pack_tile_bam_preprocess_failed( fd_pack_ctx_t *    ctx,
                                 fd_txn_m_t const * txnm ) {
  ulong bam_bundle_id = ((ulong)txnm->bam.seq_id)+1UL;

  if( FD_UNLIKELY( ctx->current_bundle_bam->is_bam ) ) {
    if( FD_UNLIKELY( ctx->current_bundle->id!=bam_bundle_id ||
                     ctx->current_bundle_bam->scheduler_gen!=txnm->bam.scheduler_gen ||
                     ctx->current_bundle_bam->ownership_gen!=txnm->bam.ownership_gen ) ) {
      pack_tile_abandon_current_bam_bundle( ctx, PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_NEW_SEQ_BEFORE_COMPLETE );
    } else {
      FD_TEST( ctx->current_bundle->txn_received==txnm->bam.batch_idx );
      if( FD_LIKELY( ctx->current_bundle->bundle ) )
        fd_pack_insert_bundle_cancel( ctx->pack, ctx->current_bundle->bundle, ctx->current_bundle->txn_cnt );
      ctx->current_bundle->bundle = NULL;
      ctx->current_bundle_bam->is_bam = 0;
    }
  }

  fd_bam_bundle_result_t res = fd_bam_result_base( txnm->bam.seq_id,
                                                   txnm->bam.scheduler_gen,
                                                   txnm->bam.max_schedule_slot,
                                                   txnm->bam.txn_cnt );
  res.bundle_err   = FD_BAM_BUNDLE_ERR_DESER;
  res.deser_index  = txnm->bam.batch_idx;
  res.deser_reason = bam_types_DeserializationErrorReason_SANITIZE_ERROR;
  pack_tile_enqueue_bam_result( ctx, &res );
  ctx->bundle_kind = PACK_TILE_BUNDLE_KIND_NONE;
}

/* pack_tile_bam_bundle_spot starts or continues assembling the BAM
   batch txnm belongs to and points cur_spot at its member.  sig is the
   resolv frag sig (blockhash height).  Returns 0 if during_frag is done
   with the frag (dropped, or a member of a batch that already failed). */
static inline int
pack_tile_bam_bundle_spot( fd_pack_ctx_t *    ctx,
                           fd_txn_m_t const * txnm,
                           ulong              sig ) {
  ulong bam_bundle_id = ((ulong)txnm->bam.seq_id)+1UL;
  FD_TEST( txnm->block_engine.bundle_id==fd_ulong_if( txnm->bam.revert_on_error, bam_bundle_id, 0UL ) );
  FD_TEST( txnm->block_engine.bundle_txn_cnt==fd_ulong_if( txnm->bam.revert_on_error && !txnm->bam.batch_idx, (ulong)txnm->bam.txn_cnt, 0UL ) );
  FD_TEST( txnm->bam.revert_on_error || txnm->bam.txn_cnt==1U );

  if( FD_UNLIKELY( txnm->bam.batch_idx &&
                   !ctx->current_bundle->bundle &&
                   ctx->current_bundle_bam->is_bam &&
                   ctx->current_bundle->id==bam_bundle_id &&
                   ctx->current_bundle_bam->scheduler_gen==txnm->bam.scheduler_gen &&
                   ctx->current_bundle_bam->ownership_gen==txnm->bam.ownership_gen &&
                   txnm->bam.batch_idx==ctx->current_bundle->txn_received ) ) {
    ctx->bundle_kind = PACK_TILE_BUNDLE_KIND_BAM;
    return 0;
  }

  if( FD_UNLIKELY( txnm->bam.batch_idx &&
                   ( !ctx->current_bundle->bundle ||
                     !ctx->current_bundle_bam->is_bam ||
                     ctx->current_bundle->id!=bam_bundle_id ||
                     ctx->current_bundle_bam->scheduler_gen!=txnm->bam.scheduler_gen ||
                     ctx->current_bundle_bam->ownership_gen!=txnm->bam.ownership_gen ||
                     txnm->bam.batch_idx!=ctx->current_bundle->txn_received ) ) ) {
    ctx->bundle_kind = PACK_TILE_BUNDLE_KIND_NONE;
    return 0;
  }

  ctx->bundle_kind = PACK_TILE_BUNDLE_KIND_BAM;

  if( FD_LIKELY( !ctx->current_bundle->bundle ||
                 !ctx->current_bundle_bam->is_bam ||
                 ctx->current_bundle->id!=bam_bundle_id ||
                 ( !txnm->bam.batch_idx && ctx->current_bundle->txn_received ) ) ) {
    if( FD_UNLIKELY( ctx->current_bundle_bam->is_bam &&
                     ctx->current_bundle->txn_received!=ctx->current_bundle->txn_cnt ) ) {
      pack_tile_abandon_current_bam_bundle( ctx, PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_NEW_SEQ_BEFORE_COMPLETE );
    } else if( FD_UNLIKELY( ctx->current_bundle->bundle ) ) {
      FD_MCNT_INC( PACK, TXN_PARTIAL_BUNDLE, ctx->current_bundle->txn_received );
      fd_pack_insert_bundle_cancel( ctx->pack, ctx->current_bundle->bundle, ctx->current_bundle->txn_cnt );
      ctx->current_bundle->bundle = NULL;
    }

    ctx->current_bundle->id                   = bam_bundle_id;
    ctx->current_bundle->txn_cnt              = txnm->bam.txn_cnt;
    ctx->current_bundle->txn_received         = 0UL;
    ctx->current_bundle->min_blockhash_height = ULONG_MAX;
    ctx->current_bundle->bundle               = fd_pack_insert_bundle_init( ctx->pack, ctx->current_bundle->_txn, txnm->bam.txn_cnt );

    ctx->current_bundle_bam->max_schedule_slot = txnm->bam.max_schedule_slot;
    ctx->current_bundle_bam->scheduler_gen     = txnm->bam.scheduler_gen;
    ctx->current_bundle_bam->ownership_gen     = txnm->bam.ownership_gen;
    ctx->current_bundle_bam->is_bam            = 1;
    ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_RECEIVED_IDX ]++;
    if( FD_LIKELY( txnm->bam.max_schedule_slot!=ULONG_MAX ) ) {
      pack_bam_recent_slot_t * entry = &ctx->bam_recent_slot[ txnm->bam.max_schedule_slot & ( FD_PACK_BAM_RECENT_SLOT_CNT - 1UL ) ];
      if( FD_UNLIKELY( ctx->dump_bam_mode==FD_BAM_DEBUG_DUMP_MODE_SLOT_FIRST &&
                       entry->slot!=txnm->bam.max_schedule_slot ) ) {
        entry->slot               = txnm->bam.max_schedule_slot;
        entry->first_debug_seq_id = txnm->bam.seq_id;
      }
    }
  }

  FD_TEST( txnm->bam.txn_cnt==ctx->current_bundle->txn_cnt );
  ctx->cur_spot = ctx->current_bundle->bundle[ txnm->bam.batch_idx ];
  if( FD_UNLIKELY( sig<ctx->current_bundle->min_blockhash_height ) ) {
    ctx->current_bundle->min_blockhash_height = sig;
  }
  return 1;
}

/* BAM part of becoming leader: evict pending work invalid for the new
   slot and forget dispatched work of earlier slots. */
static inline void
pack_tile_bam_leader_start( fd_pack_ctx_t * ctx ) {
  pack_tile_evict_invalid_pending_bam_work( ctx, 0 );
  for( ulong i=0UL; i<ctx->bam_scheduled_work_cnt; ) {
    if( FD_UNLIKELY( ctx->bam_work[ i ].max_schedule_slot==ULONG_MAX || ctx->bam_work[ i ].max_schedule_slot < ctx->leader_slot ) ) {
      pack_tile_bam_work_swap_remove( ctx, i );
      continue;
    }
    i++;
  }
}

/* pack_tile_bam_executed_txn retires tracked BAM work on an executed_txn
   frag.  Returns 1 if pack should then delete the transaction as
   executed. */
static inline int
pack_tile_bam_executed_txn( fd_pack_ctx_t * ctx,
                            ulong           sig ) {
  if( FD_UNLIKELY( sig>FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED ) ) return 0;
  int completed_unlanded = sig==FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED;

  uchar scheduled_matched_idx = UCHAR_MAX;
  ulong scheduled_work_idx = ctx->bam_work_cnt;
  if( FD_LIKELY( ctx->bam_scheduled_work_cnt ) )
    scheduled_work_idx = pack_tile_bam_work_find( ctx, ctx->executed_txn_sig, 1, &scheduled_matched_idx );
  if( FD_UNLIKELY( scheduled_work_idx<ctx->bam_work_cnt ) ) {
    pack_bam_work_t * item = &ctx->bam_work[ scheduled_work_idx ];
    pack_tile_bam_index_remove_one( ctx, scheduled_work_idx, scheduled_matched_idx );
    item->saw_unlanded_completion |= (uchar)completed_unlanded;
    if( FD_UNLIKELY( !item->indexed_mask ) ) {
      ulong completed_stage = item->saw_unlanded_completion
                            ? FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_COMPLETED_UNLANDED_IDX
                            : FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_LANDED_IDX;
      pack_tile_bam_work_swap_remove( ctx, scheduled_work_idx );
      ctx->bam_work_item_stage_cnt[ completed_stage ]++;
    }
  }

  return !completed_unlanded;
}

/* The BAM scratch, appended after the upstream scratch allocations.  It
   is empty without BAM.  pack_tile_bam_scratch_new lays it out the same
   way from address l and returns its size. */
static inline ulong
pack_tile_bam_scratch_footprint( ulong                  l,
                                 fd_topo_tile_t const * tile ) {
  ulong bam_work_max = pack_tile_bam_work_max( tile );
  l = FD_LAYOUT_APPEND( l, alignof(pack_bam_work_t), bam_work_max*sizeof(pack_bam_work_t) );
  l = FD_LAYOUT_APPEND( l, alignof(pack_bam_sig_ele_t), bam_work_max*FD_PACK_MAX_TXN_PER_BUNDLE*sizeof(pack_bam_sig_ele_t) );
  l = FD_LAYOUT_APPEND( l, pack_bam_sig_map_align(), pack_bam_sig_map_footprint( pack_tile_bam_sig_map_chain_cnt( bam_work_max ) ) );
  l = FD_LAYOUT_APPEND( l, alignof(fd_bam_bundle_result_t), 2UL*bam_work_max*sizeof(fd_bam_bundle_result_t) );
  return l;
}

static ulong
pack_tile_bam_scratch_new( fd_pack_ctx_t *        ctx,
                           fd_topo_tile_t const * tile,
                           ulong                  scratch,
                           fd_rng_t *             rng ) {
  FD_SCRATCH_ALLOC_INIT( l, scratch );
  ctx->bam_work_max = pack_tile_bam_work_max( tile );
  ctx->bam_work = FD_SCRATCH_ALLOC_APPEND( l,
                                           alignof(pack_bam_work_t),
                                           ctx->bam_work_max*sizeof(pack_bam_work_t) );
  ctx->bam_sig_pool = FD_SCRATCH_ALLOC_APPEND( l,
                                               alignof(pack_bam_sig_ele_t),
                                               ctx->bam_work_max*FD_PACK_MAX_TXN_PER_BUNDLE*sizeof(pack_bam_sig_ele_t) );
  ulong bam_sig_map_chain_cnt = pack_tile_bam_sig_map_chain_cnt( ctx->bam_work_max );
  ctx->bam_sig_map = pack_bam_sig_map_join( pack_bam_sig_map_new( FD_SCRATCH_ALLOC_APPEND( l,
                                                                                            pack_bam_sig_map_align(),
                                                                                            pack_bam_sig_map_footprint( bam_sig_map_chain_cnt ) ),
                                                                   bam_sig_map_chain_cnt,
                                                                   fd_rng_ulong( rng ) ) );
  if( FD_UNLIKELY( !ctx->bam_sig_map ) ) FD_LOG_ERR(( "pack_bam_sig_map_new failed" ));
  ctx->bam_result_queue = FD_SCRATCH_ALLOC_APPEND( l,
                                                   alignof(fd_bam_bundle_result_t),
                                                   2UL*ctx->bam_work_max*sizeof(fd_bam_bundle_result_t) );
  return FD_SCRATCH_ALLOC_FINI( l, 1UL )-scratch;
}

/* BAM part of unprivileged_init after the scratch allocations: BAM
   state, outputs and shared objects, and BAM metrics. */
static void
pack_tile_bam_init( fd_pack_ctx_t *        ctx,
                    fd_topo_t const *      topo,
                    fd_topo_tile_t const * tile ) {
  ctx->dump_bam_mode                 = tile->pack.dump_bam_mode;
  ctx->bam_work_cnt                  = 0UL;
  ctx->bam_result_queue_head         = 0UL;
  ctx->bam_scheduled_work_cnt        = 0UL;
  ctx->bam_pending_result_cnt        = 0UL;
  ctx->bam_pack_bundle_evicted_cnt   = fd_pack_bundle_evicted_cnt( ctx->pack );

  fd_pack_out_ctx_t * bam_out[2] = { &ctx->bam_leader_out, &ctx->bam_result_out };
  char const * bam_out_name[2] = { "pack_bam_ldr", "pack_bam_res" };
  for( ulong i=0UL; i<2UL; i++ ) {
    fd_pack_out_ctx_t * out = bam_out[i];
    *out = (fd_pack_out_ctx_t){ .out_idx = fd_topo_find_tile_out_link( topo, tile, bam_out_name[i], tile->kind_id ) };
    if( FD_UNLIKELY( out->out_idx==ULONG_MAX ) ) continue;
    fd_topo_link_t const * link = &topo->links[ tile->out_link_id[ out->out_idx ] ];
    out->mem    = topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ].wksp;
    out->chunk0 = fd_dcache_compact_chunk0( out->mem, link->dcache );
    out->wmark  = fd_dcache_compact_wmark ( out->mem, link->dcache, link->mtu );
    out->chunk  = out->chunk0;
  }

  ulong bam_fee_cfg_obj_id = fd_pod_query_ulong( topo->props, "bam_fee_cfg", ULONG_MAX );
  ctx->bam_fee_cfg = FD_LIKELY( bam_fee_cfg_obj_id!=ULONG_MAX )
                   ? (fd_bam_fee_cfg_t const *)fd_topo_obj_laddr( topo, bam_fee_cfg_obj_id )
                   : NULL;

  ulong bam_status_obj_id = fd_pod_query_ulong( topo->props, "bam_status", ULONG_MAX );
  ctx->bam_status_fseq = FD_LIKELY( bam_status_obj_id!=ULONG_MAX )
                       ? fd_fseq_join( fd_topo_obj_laddr( topo, bam_status_obj_id ) )
                       : NULL;
  if( FD_UNLIKELY( bam_status_obj_id!=ULONG_MAX && !ctx->bam_status_fseq ) ) FD_LOG_ERR(( "pack tile missing bam_status fseq" ));
  ctx->bam_override_snapshot = pack_tile_bam_override_active( ctx );
  ctx->bam_min_admission_slot = 0UL;
  ctx->bam_ib_associated = 0;
  ctx->bam_tip_cfg_version = ctx->bam_crank_cfg_version = 0U;
  ctx->bam_crank_next_ticks = 0L;
  ctx->bam_candidate_identity_mismatch_cnt = 0UL;

  ctx->bam_gen_fseq = NULL;
  ulong bam_gen_obj_id = fd_pod_query_ulong( topo->props, "bam_gen", ULONG_MAX );
  if( FD_LIKELY( bam_gen_obj_id!=ULONG_MAX ) ) {
    ctx->bam_gen_fseq = fd_fseq_join( fd_topo_obj_laddr( topo, bam_gen_obj_id ) );
    if( FD_UNLIKELY( !ctx->bam_gen_fseq ) ) FD_LOG_ERR(( "pack tile missing bam_gen fseq" ));
    ctx->bam_ownership_gen = (ushort)(fd_fseq_query( ctx->bam_gen_fseq )>>1);
  }

  memset( ctx->bam_bundle_assembly_abandon_cnt, '\0', sizeof(ctx->bam_bundle_assembly_abandon_cnt) );
  memset( ctx->bam_work_rejected_pre_pending_cnt, '\0', sizeof(ctx->bam_work_rejected_pre_pending_cnt) );
  memset( ctx->bam_pending_work_evicted_cnt, '\0', sizeof(ctx->bam_pending_work_evicted_cnt) );
  memset( ctx->bam_work_item_stage_cnt, '\0', sizeof(ctx->bam_work_item_stage_cnt) );
  memset( ctx->bam_work_first_outcome_cnt, '\0', sizeof(ctx->bam_work_first_outcome_cnt) );
  ctx->bam_tracking_rejected_cnt     = 0UL;
  ctx->bam_tracking_rejected_txn_cnt = 0UL;
  for( ulong i=0UL; i<FD_PACK_BAM_RECENT_SLOT_CNT; i++ ) ctx->bam_recent_slot[ i ].slot = ULONG_MAX;
  ctx->bam_current_slot_has_bam_work = 0U;
  ctx->bam_first_insert_seen = 0U;
  ctx->bam_first_schedule_seen = 0U;
  ctx->bam_first_insert_minus_slot_end_ns = 0L;
  ctx->bam_first_schedule_minus_slot_end_ns = 0L;
  memset( ctx->bam_first_insert_result_cnt,   0, sizeof(ctx->bam_first_insert_result_cnt) );
  memset( ctx->bam_first_schedule_result_cnt, 0, sizeof(ctx->bam_first_schedule_result_cnt) );
  fd_histf_join( fd_histf_new( ctx->bam_work_rx_to_first_outcome_nanos,
                               FD_MHIST_MIN( PACK, BAM_WORK_RX_TO_FIRST_OUTCOME_NANOS ),
                               FD_MHIST_MAX( PACK, BAM_WORK_RX_TO_FIRST_OUTCOME_NANOS ) ) );
  memset( ctx->current_bundle_bam,        '\0', sizeof(ctx->current_bundle_bam)        );
  memset( ctx->bam_fee_meta,              '\0', sizeof(ctx->bam_fee_meta)              );
  ctx->bam_fee_cfg_version      = 0U;
  ctx->last_bam_leader_state.slot = ULONG_MAX;

  /* The callbacks run a copy without BAM unless bam_status_fseq
     (after_credit) or bam_work_max (before_credit and the frag
     callbacks) is set, so BAM must be configured all or nothing. */
  int bam = !!ctx->bam_status_fseq;
  FD_TEST( bam==!!ctx->bam_gen_fseq && bam==!!ctx->bam_work_max &&
           bam==(ctx->bam_leader_out.out_idx!=ULONG_MAX) && bam==(ctx->bam_result_out.out_idx!=ULONG_MAX) );
}
