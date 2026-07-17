#ifndef HEADER_fd_src_disco_bam_fd_bam_types_h
#define HEADER_fd_src_disco_bam_fd_bam_types_h

#include "../pack/fd_pack.h" /* FD_PACK_MAX_TXN_PER_BUNDLE */
#include "proto/bam_types.pb.h"
#include "../../util/net/fd_net_headers.h"
#include "../metrics/generated/fd_metrics_enums.h"
#include "../../flamenco/runtime/fd_runtime_err.h"

#define FD_BAM_ERR_MSG_BUNDLE_EXECUTION_FAILED  "bundle execution failed"

/* FD_BAM_MAX_PENDING_RESULTS is the bundle result queue depth, so long disconnects
 * don't drop SchedulerMessage payloads. */
#define FD_BAM_MAX_PENDING_RESULTS 2048U
#define FD_BAM_MAX_TXN_PER_ATOMIC_BATCH             5U
/* Stem burst sizing is independent from scheduler message decode capacity. */
#define FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST   8U
#define FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE       128U
#define FD_BAM_MAX_TXN_PER_MESSAGE                  ((ulong)FD_BAM_MAX_TXN_PER_ATOMIC_BATCH * (ulong)FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE)
#define FD_BAM_STEM_BURST                           ((ulong)FD_BAM_MAX_TXN_PER_ATOMIC_BATCH * (ulong)FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST)
#define FD_BAM_BUNDLE_ERR_NONE            (0U)
#define FD_BAM_BUNDLE_ERR_DESER           (1U)

FD_STATIC_ASSERT( FD_BAM_MAX_TXN_PER_ATOMIC_BATCH <= FD_PACK_MAX_TXN_PER_BUNDLE,
                  bam_atomic_batch_limit_fits_pack_bundle );

#define FD_BAM_SHRED_SOCK_MAX          32UL

FD_STATIC_ASSERT( FD_BAM_SHRED_SOCK_MAX==sizeof(((bam_types_BamConfig *)0)->shred_sock)/sizeof(bam_types_Socket),
                  bam_shred_sock_limit_matches_protobuf );

FD_STATIC_ASSERT( _bam_types_TransactionErrorReason_MAX <= UCHAR_MAX, bam_transaction_error_reason_fits_uchar );

typedef struct {
  uint  seq_id;            /* Node-generated identifier used for ordering and result correlation. All uint values are valid. */
  ushort scheduler_gen;    /* Internal BAM scheduler identity generation; not encoded on the protobuf wire. */
  ulong slot;              /* Slot associated with the batch. Executed bundles use the bank/Poh slot; not-committed results echo the scheduler max_schedule_slot. */
  uchar bundle_txn_cnt;    /* Declared transaction count from the scheduler, capped to FD_PACK_MAX_TXN_PER_BUNDLE. Also the number of per-transaction result entries populated below; consumers must only examine indices [0,bundle_txn_cnt). */
  _Bool execution_success; /* Batch committed flag. Set to true only when the whole batch is committed. Note: individual committed transactions can still have execution_success=false (fees-only / instruction error), encoded via TransactionCommittedResult.execution_success. */
  ushort scheduling_error; /* bam_types_SchedulingError reason code when the batch never scheduled; FD_BAM_SCHED_ERR_NONE when scheduling succeeded or the bundle executed. */
  uchar transaction_err[ FD_PACK_MAX_TXN_PER_BUNDLE ]; /* Per-transaction bam_types_TransactionErrorReason for indices <bundle_txn_cnt. */
  uchar transaction_err_count; /* Number of transaction errors. 0 denotes success. */
  uint  consumed_cus    [ FD_PACK_MAX_TXN_PER_BUNDLE ]; /* Actual compute units consumed per transaction (exec+account data), even when the bundle later reverts. 0 when the txn never executed. */
  ulong feepayer_balance_lamports[ FD_PACK_MAX_TXN_PER_BUNDLE ]; /* Fee payer post-balance per transaction. Only meaningful for committed results. */
  uint  loaded_accounts_data_size[ FD_PACK_MAX_TXN_PER_BUNDLE ]; /* Loaded accounts data size (bytes) per transaction. Only meaningful for committed results. */
  _Bool sanitize_success[ FD_PACK_MAX_TXN_PER_BUNDLE ];  /* Boolean sanitize outcome per transaction (true=passed bank sanitize, false=failed). When false, transaction_err typically reports SANITIZE_FAILURE. */
  uchar bundle_err;        /* FD_BAM_BUNDLE_ERR_* selector for bundle-level rejection prior to execution. */
  uchar deser_index;       /* Zero-based transaction index tied to the deserialization error; only valid when bundle_err==FD_BAM_BUNDLE_ERR_DESER. */
  uchar deser_reason;      /* bam_types_DeserializationErrorReason enumerator for the failure reported by deser_index; only meaningful when bundle_err==FD_BAM_BUNDLE_ERR_DESER. */
} fd_bam_bundle_result_t;

typedef struct {
  ulong  slot;
  long   slot_end_ns;
  uint   slot_cu_budget_remaining;
  ushort tick;
  uchar  current_slot_has_bam_work; /* Latched once BAM work for the live leader slot arrives before slot end. */
} fd_bam_leader_state_t;

FD_STATIC_ASSERT( sizeof(fd_bam_leader_state_t)==24UL, fd_bam_leader_state_t );

FD_FN_PURE static inline _Bool
fd_bam_leader_state_eq( fd_bam_leader_state_t const * a,
                        fd_bam_leader_state_t const * b ) {
  return !!( a->slot                     == b->slot                     &&
             a->tick                     == b->tick                     &&
             a->slot_cu_budget_remaining == b->slot_cu_budget_remaining &&
             a->slot_end_ns              == b->slot_end_ns              &&
             a->current_slot_has_bam_work== b->current_slot_has_bam_work );
}

/* Mutually exclusive ownership states for BAM/Block Engine publication. */
#define FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE   (1UL<<0)
#define FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING (1UL<<1)

typedef struct {
  fd_ip4_port_t tpu;               /* TPU socket advertised by BAM (net order). */
  fd_ip4_port_t tpu_fwd;           /* TPU fwd socket advertised by BAM (net order). */
  ushort        version_client_id; /* ContactInfo version.client to advertise. */
} fd_bam_contact_update_t;

typedef struct {
  uchar         shred_sock_cnt;                       /* Number of active BAM shred receivers. */
  fd_ip4_port_t shred_sock[ FD_BAM_SHRED_SOCK_MAX ]; /* BAM shred receivers in net order. */
} fd_bam_shred_update_t;

typedef struct {
  uchar builder_pubkey[ 32 ]; /* Decoded block builder pubkey */
  uint  builder_commission;   /* Block builder commission in percentage points (0-100) */
  uint  version;              /* Seqlock version; zero means no complete snapshot and bit 31 means write-in-progress. */
} fd_bam_fee_cfg_t;

#define FD_BAM_STEM_SIG_GOSSIP_UPDATE (0xAB) // randomly assigned
#define FD_BAM_STEM_SIG_SHRED_UPDATE  (0xAC) // randomly assigned

#define FD_BAM_SCHED_ERR_NONE            USHORT_MAX
#define FD_BAM_SCHED_ERR_POH_TIMEOUT     bam_types_SchedulingError_POH_TIMEOUT
#define FD_BAM_SCHED_ERR_OUTSIDE_SLOT    bam_types_SchedulingError_OUTSIDE_LEADER_SLOT
#define FD_BAM_SCHED_ERR_CONTAINER_FULL  bam_types_SchedulingError_CONTAINER_FULL

static inline fd_bam_bundle_result_t
fd_bam_result_base( uint  seq_id,
                    ushort scheduler_gen,
                    ulong slot,
                    uchar txn_cnt ) {
  return (fd_bam_bundle_result_t) {
    .seq_id           = seq_id,
    .scheduler_gen    = scheduler_gen,
    .slot             = slot,
    .bundle_txn_cnt   = txn_cnt,
    .scheduling_error = FD_BAM_SCHED_ERR_NONE,
    .bundle_err       = FD_BAM_BUNDLE_ERR_NONE,
  };
}

static inline void
fd_bam_result_mark_sanitize_success( fd_bam_bundle_result_t * res,
                                     ulong                    idx ) {
  if( FD_LIKELY( idx<FD_PACK_MAX_TXN_PER_BUNDLE ) ) res->sanitize_success[ idx ] = 1U;
}

static inline void
fd_bam_result_mark_sanitize_success_all( fd_bam_bundle_result_t * res ) {
  for( uchar i=0U; i<res->bundle_txn_cnt; i++ ) res->sanitize_success[ i ] = 1U;
}

static inline void
fd_bam_result_set_txn_error( fd_bam_bundle_result_t *      res,
                             ulong                         idx,
                             bam_types_TransactionErrorReason reason ) {
  if( FD_LIKELY( idx<FD_PACK_MAX_TXN_PER_BUNDLE ) ) res->transaction_err[ idx ] = (uchar)reason;
}

static inline void
fd_bam_result_add_txn_error( fd_bam_bundle_result_t *      res,
                             ulong                         idx,
                             bam_types_TransactionErrorReason reason ) {
  if( FD_UNLIKELY( idx>=FD_PACK_MAX_TXN_PER_BUNDLE ) ) return;
  fd_bam_result_set_txn_error( res, idx, reason );
  res->transaction_err_count++;
}

static inline void
fd_bam_result_mark_not_committed_txn_error( fd_bam_bundle_result_t *      res,
                                            ulong                         idx,
                                            bam_types_TransactionErrorReason reason ) {
  res->execution_success = 0U;
  if( FD_LIKELY( res->transaction_err_count!=res->bundle_txn_cnt ) ) {
    for( uchar i=0U; i<res->bundle_txn_cnt; i++ )
      fd_bam_result_set_txn_error( res, i, bam_types_TransactionErrorReason_COMMIT_CANCELLED );
    res->transaction_err_count = res->bundle_txn_cnt;
  }
  fd_bam_result_set_txn_error( res, idx, reason );
}

static inline bam_types_TransactionErrorReason
fd_bam_txn_err_from_pack_insert( int pack_rc ) {
  switch( pack_rc ) {
  case FD_PACK_INSERT_REJECT_DUPLICATE:        return bam_types_TransactionErrorReason_ALREADY_PROCESSED;
  case FD_PACK_INSERT_REJECT_UNAFFORDABLE:     return bam_types_TransactionErrorReason_INSUFFICIENT_FUNDS_FOR_FEE;
  case FD_PACK_INSERT_REJECT_ADDR_LUT:         return bam_types_TransactionErrorReason_ADDRESS_LOOKUP_TABLE_NOT_FOUND;
  case FD_PACK_INSERT_REJECT_EXPIRED:          return bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND;
  case FD_PACK_INSERT_REJECT_TOO_LARGE:        return bam_types_TransactionErrorReason_WOULD_EXCEED_MAX_BLOCK_COST_LIMIT;
  case FD_PACK_INSERT_REJECT_ACCOUNT_CNT:      return bam_types_TransactionErrorReason_TOO_MANY_ACCOUNT_LOCKS;
  case FD_PACK_INSERT_REJECT_DUPLICATE_ACCT:   return bam_types_TransactionErrorReason_ACCOUNT_LOADED_TWICE;
  case FD_PACK_INSERT_REJECT_ESTIMATION_FAIL:  return bam_types_TransactionErrorReason_SANITIZE_FAILURE;
  case FD_PACK_INSERT_REJECT_WRITES_SYSVAR:    return bam_types_TransactionErrorReason_INVALID_WRITABLE_ACCOUNT;
  case FD_PACK_INSERT_REJECT_INVALID_NONCE:    return bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND;
  case FD_PACK_INSERT_REJECT_BUNDLE_BLACKLIST: return bam_types_TransactionErrorReason_PROGRAM_EXECUTION_TEMPORARILY_RESTRICTED;
  case FD_PACK_INSERT_REJECT_NONCE_CONFLICT:   return bam_types_TransactionErrorReason_SANITIZE_FAILURE;
  default:                                     return bam_types_TransactionErrorReason_COMMIT_CANCELLED;
  }
}

static inline bam_types_TransactionErrorReason
fd_bam_txn_err_from_transaction_error_idx( int transaction_err_idx ) {
  if( FD_LIKELY( transaction_err_idx>=FD_METRICS_ENUM_TRANSACTION_ERROR_V_ACCOUNT_IN_USE_IDX &&
                 transaction_err_idx<=FD_METRICS_ENUM_TRANSACTION_ERROR_V_COMMIT_CANCELLED_IDX ) ) {
    return (bam_types_TransactionErrorReason)( transaction_err_idx - FD_METRICS_ENUM_TRANSACTION_ERROR_V_ACCOUNT_IN_USE_IDX );
  }
  return bam_types_TransactionErrorReason_COMMIT_CANCELLED;
}

static inline bam_types_TransactionErrorReason
fd_bam_txn_err_from_runtime_err( int err ) {
  switch( err ) {
  case FD_RUNTIME_TXN_ERR_BLOCKHASH_NONCE_ALREADY_ADVANCED:
  case FD_RUNTIME_TXN_ERR_BLOCKHASH_FAIL_ADVANCE_NONCE_INSTR:
  case FD_RUNTIME_TXN_ERR_BLOCKHASH_FAIL_WRONG_NONCE:
    return bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND;
  case FD_RUNTIME_TXN_ERR_ACCOUNT_LOADED_TWICE:                     return bam_types_TransactionErrorReason_ACCOUNT_LOADED_TWICE;
  case FD_RUNTIME_TXN_ERR_ACCOUNT_NOT_FOUND:                        return bam_types_TransactionErrorReason_ACCOUNT_NOT_FOUND;
  case FD_RUNTIME_TXN_ERR_PROGRAM_ACCOUNT_NOT_FOUND:                return bam_types_TransactionErrorReason_PROGRAM_ACCOUNT_NOT_FOUND;
  case FD_RUNTIME_TXN_ERR_INSUFFICIENT_FUNDS_FOR_FEE:               return bam_types_TransactionErrorReason_INSUFFICIENT_FUNDS_FOR_FEE;
  case FD_RUNTIME_TXN_ERR_INVALID_ACCOUNT_FOR_FEE:                  return bam_types_TransactionErrorReason_INVALID_ACCOUNT_FOR_FEE;
  case FD_RUNTIME_TXN_ERR_ALREADY_PROCESSED:                        return bam_types_TransactionErrorReason_ALREADY_PROCESSED;
  case FD_RUNTIME_TXN_ERR_BLOCKHASH_NOT_FOUND:                      return bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND;
  case FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR:                        return bam_types_TransactionErrorReason_INSTRUCTION_ERROR;
  case FD_RUNTIME_TXN_ERR_SIGNATURE_FAILURE:                        return bam_types_TransactionErrorReason_SIGNATURE_FAILURE;
  case FD_RUNTIME_TXN_ERR_INVALID_PROGRAM_FOR_EXECUTION:            return bam_types_TransactionErrorReason_INVALID_PROGRAM_FOR_EXECUTION;
  case FD_RUNTIME_TXN_ERR_SANITIZE_FAILURE:                         return bam_types_TransactionErrorReason_SANITIZE_FAILURE;
  case FD_RUNTIME_TXN_ERR_WOULD_EXCEED_MAX_BLOCK_COST_LIMIT:        return bam_types_TransactionErrorReason_WOULD_EXCEED_MAX_BLOCK_COST_LIMIT;
  case FD_RUNTIME_TXN_ERR_WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT:      return bam_types_TransactionErrorReason_WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT;
  case FD_RUNTIME_TXN_ERR_WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT:    return bam_types_TransactionErrorReason_WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT;
  case FD_RUNTIME_TXN_ERR_TOO_MANY_ACCOUNT_LOCKS:                   return bam_types_TransactionErrorReason_TOO_MANY_ACCOUNT_LOCKS;
  case FD_RUNTIME_TXN_ERR_ADDRESS_LOOKUP_TABLE_NOT_FOUND:           return bam_types_TransactionErrorReason_ADDRESS_LOOKUP_TABLE_NOT_FOUND;
  case FD_RUNTIME_TXN_ERR_INVALID_ADDRESS_LOOKUP_TABLE_OWNER:       return bam_types_TransactionErrorReason_INVALID_ADDRESS_LOOKUP_TABLE_OWNER;
  case FD_RUNTIME_TXN_ERR_INVALID_ADDRESS_LOOKUP_TABLE_DATA:        return bam_types_TransactionErrorReason_INVALID_ADDRESS_LOOKUP_TABLE_DATA;
  case FD_RUNTIME_TXN_ERR_INVALID_ADDRESS_LOOKUP_TABLE_INDEX:       return bam_types_TransactionErrorReason_INVALID_ADDRESS_LOOKUP_TABLE_INDEX;
  case FD_RUNTIME_TXN_ERR_WOULD_EXCEED_MAX_VOTE_COST_LIMIT:         return bam_types_TransactionErrorReason_WOULD_EXCEED_MAX_VOTE_COST_LIMIT;
  case FD_RUNTIME_TXN_ERR_WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT:    return bam_types_TransactionErrorReason_WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT;
  case FD_RUNTIME_TXN_ERR_DUPLICATE_INSTRUCTION:                    return bam_types_TransactionErrorReason_DUPLICATE_INSTRUCTION;
  case FD_RUNTIME_TXN_ERR_INSUFFICIENT_FUNDS_FOR_RENT:              return bam_types_TransactionErrorReason_INSUFFICIENT_FUNDS_FOR_RENT;
  case FD_RUNTIME_TXN_ERR_MAX_LOADED_ACCOUNTS_DATA_SIZE_EXCEEDED:   return bam_types_TransactionErrorReason_MAX_LOADED_ACCOUNTS_DATA_SIZE_EXCEEDED;
  case FD_RUNTIME_TXN_ERR_INVALID_LOADED_ACCOUNTS_DATA_SIZE_LIMIT:  return bam_types_TransactionErrorReason_INVALID_LOADED_ACCOUNTS_DATA_SIZE_LIMIT;
  case FD_RUNTIME_TXN_ERR_UNBALANCED_TRANSACTION:                   return bam_types_TransactionErrorReason_UNBALANCED_TRANSACTION;
  default:                                                          return bam_types_TransactionErrorReason_COMMIT_CANCELLED;
  }
}

#endif /* HEADER_fd_src_disco_bam_fd_bam_types_h */
