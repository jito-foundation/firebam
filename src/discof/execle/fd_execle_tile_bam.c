/* FireBAM part of the execle tile: BAM batch results for the BAM
   tile.  Included once by fd_execle_tile.c after fd_execle_tile_t. */

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

/* bam_result_not_committed records a non-revert BAM transaction that
   did not commit. */
static inline void
bam_result_not_committed( fd_bam_bundle_result_t * bam_res,
                          fd_txn_out_t const *     txn_out ) {
  fd_bam_result_mark_not_committed_txn_error( bam_res, 0UL, fd_bam_txn_err_from_runtime_err( txn_out->err.txn_err ) );
  bam_fill_txn_result( bam_res, 0UL, txn_out );
}

/* bam_result_committed records a committed non-revert BAM transaction. */
static inline void
bam_result_committed( fd_bam_bundle_result_t * bam_res,
                      fd_txn_out_t const *     txn_out ) {
  if( FD_UNLIKELY( txn_out->err.txn_err!=FD_RUNTIME_EXECUTE_SUCCESS ) )
    fd_bam_result_add_txn_error( bam_res, 0UL, fd_bam_txn_err_from_runtime_err( txn_out->err.txn_err ) );
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
