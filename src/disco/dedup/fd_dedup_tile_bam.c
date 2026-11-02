/* FireBAM part of the dedup tile, included once by fd_dedup_tile.c.
   BAM frags reach dedup out of order and leave in bam_verif order.  BAM
   may resend a signature, so it skips the signature tcache.  A revert
   batch shares the bundle failure tracking (fd_txn_m_failure_group_id)
   and is checked for duplicate members as a Block Engine bundle is.
   Every frag not in an already failed batch is published, a failed one
   with preprocess_failed set so pack can answer the batch, and all of
   it to resolv:0 (sig 1) so batches stay ordered. */

#include "../bam/fd_bam_txn_m.h"

/* bam_verif's first frag has seq 0, as every stem in starts there */
static void
fd_dedup_tile_bam_init( fd_dedup_ctx_t * ctx,
                        ulong            in_cnt ) {
  ctx->bam_seq    = 0UL;
  ctx->verify_cnt = 0UL;
  for( ulong i=0UL; i<in_cnt; i++ ) ctx->verify_cnt += ctx->in_kind[ i ]==IN_KIND_VERIFY;
}

/* Verify tiles take bam_verif frags round robin and publish each with
   its bam_verif seq plus one as sig (other frags carry sig 0), so frag
   n reaches dedup from verify tile n%verify_cnt, maybe after later
   frags from the others.  A BAM frag stays on its in (-1 hands it back
   to the stem) until every earlier one is admitted, so dedup and all
   after it see bam_verif order.  This cannot deadlock: the frag waited
   on was published to bam_verif before the later one, and on its own
   in only frags admitted without waiting are ahead of it.  A frag
   behind the expected one, or ahead of it from the same verify tile,
   means a frag was repeated or lost, which would stall BAM for good. */
static inline int
fd_dedup_tile_bam_before_frag( fd_dedup_ctx_t * ctx,
                               ulong            in_idx,
                               ulong            seq FD_PARAM_UNUSED,
                               ulong            sig ) {
  if( FD_LIKELY( !sig || ctx->in_kind[ in_idx ]!=IN_KIND_VERIFY ) ) return 0;
  ulong bam_seq = sig-1UL;
  if( FD_UNLIKELY( bam_seq!=ctx->bam_seq ) ) {
    if( FD_UNLIKELY( bam_seq<ctx->bam_seq || bam_seq%ctx->verify_cnt==ctx->bam_seq%ctx->verify_cnt ) )
      FD_LOG_ERR(( "bam_verif frag %lu reached dedup while waiting for %lu, so a frag was lost or repeated", bam_seq, ctx->bam_seq ));
    return -1;
  }
  ctx->bam_seq++;
  return 0;
}

static void
fd_dedup_tile_bam_after_frag( fd_dedup_ctx_t *    ctx,
                              fd_txn_m_t *        txnm,
                              fd_txn_t const *    txn,
                              ulong               tsorig,
                              fd_stem_context_t * stem ) {
  ulong failure_group_id = fd_txn_m_failure_group_id( txnm );

  /* A repeated seq_id is a new batch when batch_idx returns to zero */
  if( FD_UNLIKELY( failure_group_id && ( failure_group_id!=ctx->bundle_id || !txnm->bam.batch_idx ) ) ) {
    ctx->bundle_failed = 0;
    ctx->bundle_id     = failure_group_id;
    ctx->bundle_idx    = 0UL;
  }

  if( FD_UNLIKELY( failure_group_id && ctx->bundle_failed ) ) {
    ctx->metrics.dedup_tile_result[ FD_METRICS_ENUM_DEDUP_RESULT_V_BUNDLE_PEER_FAILED_IDX ]++;
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
      ctx->metrics.dedup_tile_result[ FD_METRICS_ENUM_DEDUP_RESULT_V_DUPLICATE_IDX ]++;
      txnm->bam.preprocess_failed = 1U;
    } else {
      ctx->metrics.dedup_tile_result[ FD_METRICS_ENUM_DEDUP_RESULT_V_SUCCESS_IDX ]++;
    }
  }

  ulong realized_sz = fd_txn_m_realized_footprint( txnm, 1, 0 );
  fd_stem_publish( stem, 0UL, 1UL, ctx->out_chunk, realized_sz, 0UL, tsorig, (ulong)fd_frag_meta_ts_comp( fd_tickcount() ) );
  ctx->out_chunk = fd_dcache_compact_next( ctx->out_chunk, realized_sz, ctx->out_chunk0, ctx->out_wmark );
}
