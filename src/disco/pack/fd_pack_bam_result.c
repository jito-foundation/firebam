#include "fd_pack_bam_result.h"

#include <stdio.h>

void
fd_pack_assign_bam_failure_reason( fd_bam_bundle_result_t * res,
                                   ushort                   idx,
                                   int                      result ) {
  if( FD_UNLIKELY( !res ) ) return;
  if( FD_UNLIKELY( idx>=FD_PACK_MAX_TXN_PER_BUNDLE ) ) idx = (uint)FD_PACK_MAX_TXN_PER_BUNDLE - 1U;
  ushort new_cnt = idx + 1;
  if( FD_UNLIKELY( res->txn_cnt<=idx ) )
    res->txn_cnt = (uchar)fd_uint_min( new_cnt, (uint)FD_PACK_MAX_TXN_PER_BUNDLE );
  if( FD_UNLIKELY( res->bundle_txn_cnt<=idx ) )
    res->bundle_txn_cnt = (uchar)fd_uint_min( new_cnt, (uint)FD_PACK_MAX_TXN_PER_BUNDLE );
  res->sanitize_success[ idx ] = 1;

  switch( result ) {
  case FD_PACK_INSERT_REJECT_NONCE_PRIORITY:
    res->has_deser_error = 1;
    res->deser_reason    = bam_types_DeserializationErrorReason_PRIORITIZATION_FAILURE;
    res->deser_index     = (uchar)idx;
    break;
  case FD_PACK_INSERT_REJECT_UNAFFORDABLE:
    res->transaction_err[ idx ] = bam_types_TransactionErrorReason_INSUFFICIENT_FUNDS_FOR_FEE;
    break;
  case FD_PACK_INSERT_REJECT_ADDR_LUT:
    res->transaction_err[ idx ] = bam_types_TransactionErrorReason_ADDRESS_LOOKUP_TABLE_NOT_FOUND;
    break;
  case FD_PACK_INSERT_REJECT_DUPLICATE:
  case FD_PACK_INSERT_REJECT_DUPLICATE_ACCT:
  case FD_PACK_INSERT_REJECT_TOO_LARGE:
  case FD_PACK_INSERT_REJECT_ACCOUNT_CNT:
  case FD_PACK_INSERT_REJECT_ESTIMATION_FAIL:
  case FD_PACK_INSERT_REJECT_WRITES_SYSVAR:
  case FD_PACK_INSERT_REJECT_INVALID_NONCE:
  case FD_PACK_INSERT_REJECT_BUNDLE_BLACKLIST:
  case FD_PACK_INSERT_REJECT_NONCE_CONFLICT:
    res->has_deser_error = 1;
    res->deser_reason    = bam_types_DeserializationErrorReason_FILTER_FAILURE;
    res->deser_index     = (uchar)idx;
    break;
  default:
    if( FD_LIKELY( !res->has_generic_invalid ) ) {
      res->has_generic_invalid = 1;
      snprintf( res->generic_invalid_msg,
                FD_BAM_GENERIC_INVALID_MSG_MAX,
                "pack rejected seq_id %u idx %u (code %d)",
                res->seq_id,
                idx,
                result );
    }
    break;
  }
}
