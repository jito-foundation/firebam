/* FireBAM part of the execle tile: BAM batch results for the BAM
   tile.  Included once by fd_execle_tile.c after fd_execle_tile_t. */

#include "../../disco/bam/fd_bam_microblock.h"

/* bam_result_start starts *res, optimistically successful, if the
   microblock of txn_cnt transactions at txns is a BAM batch with
   revert_on_error==revert.  A revert batch arrives as a bundle, and BAM
   rejects multi-transaction non-revert batches, so a non-revert batch is
   a single transaction on the ordinary execution path. */
static inline int
bam_result_start( fd_execle_tile_t const * ctx,
                  void const *             txns,
                  ulong                    txn_cnt,
                  ulong                    slot,
                  int                      revert,
                  fd_bam_bundle_result_t * res ) {
  fd_txn_p_t const * txn = (fd_txn_p_t const *)txns;
  if( FD_LIKELY( !txn_cnt || txn->source_tpu!=FD_TXN_M_TPU_SOURCE_BAM || ctx->_bam.revert_on_error!=revert ) ) return 0;
  if( FD_UNLIKELY( !revert && txn_cnt!=1UL ) ) return 0;
  FD_TEST( txn_cnt<=FD_PACK_MAX_TXN_PER_BUNDLE );
  *res = fd_bam_result_base( ctx->_bam.seq_id, ctx->_bam.scheduler_gen, slot, (uchar)txn_cnt );
  res->execution_success = 1U;
  return 1;
}

static inline void
bam_fill_txn_result( fd_bam_bundle_result_t * res,
                     ulong                    idx,
                     fd_txn_out_t const *     txn_out ) {
  uint actual_execution_cus = 0U;
  if( FD_LIKELY( txn_out->details.compute_budget.compute_unit_limit>=txn_out->details.compute_budget.compute_meter ) )
    actual_execution_cus = (uint)(txn_out->details.compute_budget.compute_unit_limit - txn_out->details.compute_budget.compute_meter);
  /* Failed execution commits only the post-fee rollback balance. */
  ulong feepayer_balance_lamports = txn_out->accounts.fee_payer_rollback_lamports;
  if( FD_LIKELY( !txn_out->err.txn_err && txn_out->accounts.cnt && txn_out->accounts.account[ 0 ] ) )
    feepayer_balance_lamports = txn_out->accounts.account[ 0 ]->lamports;

  if( FD_LIKELY( txn_out->err.txn_err!=FD_RUNTIME_TXN_ERR_SANITIZE_FAILURE ) )
    fd_bam_result_mark_sanitize_success( res, idx );
  res->consumed_cus[ idx ]              = actual_execution_cus;
  res->feepayer_balance_lamports[ idx ] = feepayer_balance_lamports;
  res->loaded_accounts_data_size[ idx ] = (uint)fd_ulong_min( txn_out->details.loaded_accounts_data_size, (ulong)UINT_MAX );
}

/* bam_result_txn records the outcome of a non-revert BAM transaction,
   before it is committed or cancelled. */
static inline void
bam_result_txn( fd_bam_bundle_result_t * bam_res,
                fd_txn_out_t const *     txn_out ) {
  bam_types_TransactionErrorReason err = fd_bam_txn_err_from_runtime_err( txn_out->err.txn_err );
  if( FD_UNLIKELY( !txn_out->err.is_committable ) )                fd_bam_result_mark_not_committed_txn_error( bam_res, 0UL, err );
  else if( FD_UNLIKELY( txn_out->err.txn_err!=FD_RUNTIME_EXECUTE_SUCCESS ) ) fd_bam_result_add_txn_error( bam_res, 0UL, err );
  bam_fill_txn_result( bam_res, 0UL, txn_out );
}

static inline void
bam_publish_result( fd_execle_tile_t *             ctx,
                    fd_stem_context_t *            stem,
                    fd_bam_bundle_result_t const * bam_res ) {
  fd_bam_publish_result( stem, ctx->out_bam->idx, ctx->out_bam->mem, &ctx->out_bam->chunk,
                         ctx->out_bam->chunk0, ctx->out_bam->wmark, bam_res );
}

/* bam_bundle_failed_result builds and publishes the result of a BAM
   revert batch that failed at failed_idx (setup_bundle is 0 if its
   accounts could not be prepared). */
static inline void
bam_bundle_failed_result( fd_execle_tile_t *       ctx,
                          fd_stem_context_t *      stem,
                          fd_bam_bundle_result_t * bam_res,
                          ulong                    txn_cnt,
                          ulong                    failed_idx,
                          int                      setup_bundle ) {
  bam_res->execution_success = 0U;
  for( ulong i=0UL; i<txn_cnt; i++ ) {
    if( FD_LIKELY( i!=failed_idx || ctx->txn_out[ failed_idx ].err.txn_err!=FD_RUNTIME_TXN_ERR_SANITIZE_FAILURE ) )
      fd_bam_result_mark_sanitize_success( bam_res, i );
    fd_bam_result_add_txn_error( bam_res, i, bam_types_TransactionErrorReason_COMMIT_CANCELLED );
  }
  fd_bam_result_set_txn_error( bam_res, failed_idx, fd_bam_txn_err_from_runtime_err( ctx->txn_out[ failed_idx ].err.txn_err ) );
  if( FD_LIKELY( setup_bundle ) )
    for( ulong i=0UL; i<=failed_idx; i++ ) bam_fill_txn_result( bam_res, i, &ctx->txn_out[ i ] );
  bam_publish_result( ctx, stem, bam_res );
}
