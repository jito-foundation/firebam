/* FireBAM part of the dedup tile: after_frag for BAM transactions.
   Included once by fd_dedup_tile.c after fd_dedup_ctx_t, before
   after_frag.

   BAM traffic is sequenced by the BAM node and may resend a signature,
   so it skips the signature tcache.  A revert batch shares upstream's
   bundle failure tracking, in a namespace disjoint from Block Engine
   bundle ids (fd_txn_m_failure_group_id), and is checked for duplicate
   members as a Block Engine bundle is.  Every frag that does not belong
   to an already failed batch is published, a failed one with
   preprocess_failed set so pack can answer the batch, and all BAM
   traffic routes to resolv:0 so batches stay ordered.  BAM never
   arrives on the gossip link. */

static void
fd_dedup_tile_bam_after_frag( fd_dedup_ctx_t *    ctx,
                              fd_txn_m_t *        txnm,
                              fd_txn_t const *    txn,
                              ulong               tsorig,
                              fd_stem_context_t * stem ) {
  ulong failure_group_id = fd_txn_m_failure_group_id( txnm );

  /* BAM derives bundle_id from seq_id; a repeated seq_id is still a new
     BAM-batch boundary when batch_idx returns to zero. */
  if( FD_UNLIKELY( failure_group_id && ( failure_group_id!=ctx->bundle_id || !txnm->bam.batch_idx ) ) ) {
    ctx->bundle_failed = 0;
    ctx->bundle_id     = failure_group_id;
    ctx->bundle_idx    = 0UL;
  }

  if( FD_UNLIKELY( failure_group_id && ctx->bundle_failed ) ) {
    ctx->metrics.dedup_tile_result[ FD_METRICS_ENUM_DEDUP_TILE_RESULT_V_BUNDLE_PEER_FAILURE_IDX ]++;
    return;
  }

  if( FD_UNLIKELY( txnm->bam.preprocess_failed ) ) {
    if( FD_LIKELY( failure_group_id ) ) ctx->bundle_failed = 1;
  } else {
    int is_dup = 0;
    if( FD_UNLIKELY( failure_group_id ) ) {
      if( FD_UNLIKELY( ctx->bundle_idx>4UL ) ) FD_LOG_ERR(( "bundle_idx %lu > 4", ctx->bundle_idx ));
      for( ulong i=0UL; i<ctx->bundle_idx; i++ ) {
        if( !memcmp( ctx->bundle_signatures[ i ], fd_txn_m_payload( txnm )+txn->signature_off, 64UL ) ) {
          is_dup = 1;
          break;
        }
      }
      if( FD_UNLIKELY( ctx->bundle_idx==4UL ) ) ctx->bundle_idx++;
      else fd_memcpy( ctx->bundle_signatures[ ctx->bundle_idx++ ], fd_txn_m_payload( txnm )+txn->signature_off, 64UL );
    }

    if( FD_UNLIKELY( is_dup ) ) {
      ctx->bundle_failed = 1;
      ctx->metrics.dedup_tile_result[ FD_METRICS_ENUM_DEDUP_TILE_RESULT_V_DEDUP_FAILURE_IDX ]++;
      txnm->bam.preprocess_failed = 1U;
    } else {
      ctx->metrics.dedup_tile_result[ FD_METRICS_ENUM_DEDUP_TILE_RESULT_V_SUCCESS_IDX ]++;
    }
  }

  ulong realized_sz = fd_txn_m_realized_footprint( txnm, 1, 0 );
  ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_tickcount() );
  fd_stem_publish( stem, 0UL, 1UL, ctx->out_chunk, realized_sz, 0UL, tsorig, tspub );
  ctx->out_chunk = fd_dcache_compact_next( ctx->out_chunk, realized_sz, ctx->out_chunk0, ctx->out_wmark );
}
