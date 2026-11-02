/* FireBAM part of the verify tile, included once by fd_verify_tile.c.
   BAM txns arrive parsed and skip the HA dedup tcache (BAM may resend a
   txn that did not land).  Verify tiles take bam_verif frags round
   robin, so the members of a batch can land on different tiles.  Each
   frag is checked on its own and always published, with its bam_verif
   seq plus one as sig: dedup puts BAM frags back in bam_verif order
   and fails the members after a failed one.  A failed txn is published
   for pack with its parsed txn zeroed, so no consumer of the verified
   stream can act on an unverified txn. */

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
                               ulong               seq,
                               ulong               tsorig,
                               fd_stem_context_t * stem ) {
  if( FD_UNLIKELY( txnm->payload_sz>FD_TPU_MTU ) ) FD_LOG_ERR(( "verify: txn payload size %hu exceeds max %lu", txnm->payload_sz, FD_TPU_MTU ));
  fd_txn_t * txnt = fd_txn_m_txn_t( txnm );
  txnm->bam.preprocess_failed = 0U;

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
    txnm->bam.preprocess_failed = 1U;
    txnm->txn_t_sz = (ushort)fd_txn_footprint( 0UL, 0UL );
    fd_memset( txnt, 0, txnm->txn_t_sz );
  }

  ulong realized_sz = fd_txn_m_realized_footprint( txnm, 1, 0 );
  fd_stem_publish( stem, 0UL, seq+1UL, ctx->out_chunk, realized_sz, 0UL, tsorig, (ulong)fd_frag_meta_ts_comp( fd_tickcount() ) );
  ctx->out_chunk = fd_dcache_compact_next( ctx->out_chunk, realized_sz, ctx->out_chunk0, ctx->out_wmark );
}
