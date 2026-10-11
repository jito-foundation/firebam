/* FireBAM part of the resolv tile, included once by fd_resolv_tile.c.
   The BAM node schedules on its own view of the chain, so a BAM txn is
   never dropped for an expired blockhash or stashed for an unknown one.
   It is still published, with preprocess_failed set so pack can answer
   the batch, if it failed upstream or its lookup tables don't resolve. */

static void
fd_resolv_tile_bam_after_frag( fd_resolv_ctx_t *   ctx,
                               fd_txn_m_t *        txnm,
                               fd_txn_t const *    txnt,
                               ulong               tsorig,
                               fd_stem_context_t * stem ) {
  ulong failure_group_id = fd_txn_m_failure_group_id( txnm );

  /* A repeated seq_id is a new batch when batch_idx returns to zero */
  if( FD_UNLIKELY( failure_group_id && ( failure_group_id!=ctx->bundle_id || !txnm->bam.batch_idx ) ) ) {
    ctx->bundle_failed = 0;
    ctx->bundle_id     = failure_group_id;
  }

  if( FD_UNLIKELY( failure_group_id && ctx->bundle_failed ) ) {
    ctx->metrics.txn_dropped[ FD_METRICS_ENUM_RESOLV_TXN_DROP_REASON_V_BUNDLE_PEER_FAILED_IDX ]++;
    return;
  }

  txnm->reference_block_height = ctx->completed_block_height;
  if( FD_LIKELY( !txnm->bam.preprocess_failed ) ) {
    int is_durable_nonce = fd_disco_tpu_is_durable_nonce( txnt, fd_txn_m_payload( txnm ) );
    map_t const *           map  = fd_ptr_if( is_durable_nonce, ctx->nonce_blockhash_map, ctx->blockhash_map );
    blockhash_map_t const * pool = is_durable_nonce ? ctx->nonce_blockhash_ring : ctx->blockhash_ring;
    blockhash_map_t const * blockhash = map_ele_query_const( map, (blockhash_t const *)( fd_txn_m_payload( txnm )+txnt->recent_blockhash_off ), NULL, pool );
    if( FD_LIKELY( blockhash ) ) txnm->reference_block_height = blockhash->block_height;

    if( FD_UNLIKELY( txnt->addr_table_adtl_cnt ) ) {
      if( FD_UNLIKELY( !ctx->bank ) ) ctx->metrics.txn_dropped[ FD_METRICS_ENUM_RESOLV_TXN_DROP_REASON_V_NO_BANK_IDX ]++;
      if( FD_UNLIKELY( !ctx->bank || peek_aluts( ctx, txnm ) ) ) txnm->bam.preprocess_failed = 1U;
    }
  }
  if( FD_UNLIKELY( failure_group_id && txnm->bam.preprocess_failed ) ) ctx->bundle_failed = 1;

  ulong realized_sz = fd_txn_m_realized_footprint( txnm, 1, 1 );
  fd_stem_publish( stem, 0UL, txnm->reference_block_height, ctx->out_pack->chunk, realized_sz, 0UL, tsorig, fd_frag_meta_ts_comp( fd_tickcount() ) );
  ctx->out_pack->chunk = fd_dcache_compact_next( ctx->out_pack->chunk, realized_sz, ctx->out_pack->chunk0, ctx->out_pack->wmark );
}
