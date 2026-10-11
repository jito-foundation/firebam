/* FireBAM part of the verify tile, included once by fd_verify_tile.c.
   BAM txns arrive parsed and skip the HA dedup tcache (BAM may resend a
   txn that did not land).  A revert batch shares the bundle failure
   tracking (fd_txn_m_failure_group_id).  A failed txn is still
   published for pack, with its parsed txn zeroed so no consumer of the
   verified stream can act on an unverified txn. */

/* BAM frags arrive parsed, so they can exceed FD_TPU_RAW_MTU */
static inline void
fd_verify_tile_bam_during_frag( fd_verify_ctx_t * ctx,
                                ulong             in_idx,
                                ulong             chunk,
                                ulong             sz ) {
  if( FD_UNLIKELY( chunk<ctx->in[in_idx].chunk0 || chunk>ctx->in[in_idx].wmark || sz>FD_TPU_PARSED_MTU ) )
    FD_LOG_ERR(( "chunk %lu %lu corrupt, not in range [%lu,%lu,%lu]", chunk, sz, ctx->in[in_idx].chunk0, ctx->in[in_idx].wmark, FD_TPU_PARSED_MTU ));
  fd_memcpy( fd_chunk_to_laddr( ctx->out_mem, ctx->out_chunk ), fd_chunk_to_laddr( ctx->in[in_idx].mem, chunk ), sz );
}

static void
fd_verify_tile_bam_after_frag( fd_verify_ctx_t *   ctx,
                               fd_txn_m_t *        txnm,
                               ulong               tsorig,
                               fd_stem_context_t * stem ) {
  if( FD_UNLIKELY( txnm->payload_sz>FD_TPU_MTU ) ) FD_LOG_ERR(( "verify: txn payload size %hu exceeds max %lu", txnm->payload_sz, FD_TPU_MTU ));
  fd_txn_t * txnt = fd_txn_m_txn_t( txnm );
  txnm->bam.preprocess_failed = 0U;
  ulong failure_group_id = fd_txn_m_failure_group_id( txnm );

  /* A repeated seq_id is a new batch when batch_idx returns to zero */
  if( FD_UNLIKELY( failure_group_id && ( failure_group_id!=ctx->bundle_id || !txnm->bam.batch_idx ) ) ) {
    ctx->bundle_failed = 0;
    ctx->bundle_id     = failure_group_id;
  }

  if( FD_UNLIKELY( failure_group_id && ctx->bundle_failed ) ) {
    ctx->metrics.verify_tile_result[ FD_METRICS_ENUM_VERIFY_RESULT_V_BUNDLE_PEER_FAILED_IDX ]++;
    return;
  }

  ulong result_idx = FD_METRICS_ENUM_VERIFY_RESULT_V_PARSE_FAILED_IDX;
  if( FD_LIKELY( txnm->txn_t_sz ) ) {
    ulong _txn_sig;
    int res = fd_txn_verify( ctx, fd_txn_m_payload( txnm ), txnm->payload_sz, txnt, 0, &_txn_sig );
    result_idx = fd_ulong_if( res==FD_TXN_VERIFY_SUCCESS,
                              FD_METRICS_ENUM_VERIFY_RESULT_V_SUCCESS_IDX,
                              FD_METRICS_ENUM_VERIFY_RESULT_V_SIGNATURE_FAILED_IDX );
  }
  ctx->metrics.verify_tile_result[ result_idx ]++;

  if( FD_UNLIKELY( result_idx!=FD_METRICS_ENUM_VERIFY_RESULT_V_SUCCESS_IDX ) ) {
    if( FD_UNLIKELY( failure_group_id ) ) ctx->bundle_failed = 1;
    txnm->bam.preprocess_failed = 1U;
    txnm->txn_t_sz = (ushort)fd_txn_footprint( 0UL, 0UL );
    fd_memset( txnt, 0, txnm->txn_t_sz );
  }

  ulong realized_sz = fd_txn_m_realized_footprint( txnm, 1, 0 );
  fd_stem_publish( stem, 0UL, 0UL, ctx->out_chunk, realized_sz, 0UL, tsorig, (ulong)fd_frag_meta_ts_comp( fd_tickcount() ) );
  ctx->out_chunk = fd_dcache_compact_next( ctx->out_chunk, realized_sz, ctx->out_chunk0, ctx->out_wmark );
}
