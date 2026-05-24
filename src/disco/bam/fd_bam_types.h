#ifndef HEADER_fd_src_disco_bam_fd_bam_types_h
#define HEADER_fd_src_disco_bam_fd_bam_types_h

#include "../pack/fd_pack.h" /* FD_PACK_MAX_TXN_PER_BUNDLE */
#include "proto/bam_types.pb.h"
#include "../../util/net/fd_net_headers.h"
#include "../metrics/generated/fd_metrics_enums.h"
#include "../../flamenco/runtime/fd_runtime_err.h"

/* Central definitions for user-visible BAM error strings.
   Keep format strings and prefixes in one place so tests can
   assert on them without duplicating literals. */

#define FD_BAM_ERR_MSG_BUILDER_INFO_UNAVAILABLE "builder info unavailable"
#define FD_BAM_ERR_MSG_BUNDLE_EXECUTION_FAILED  "bundle execution failed"

#define FD_BAM_ERR_FMT_TRANSACTION_ERROR        "transaction error %u"
#define FD_BAM_ERR_PREFIX_TRANSACTION_ERROR     "transaction error "

#define FD_BAM_ERR_FMT_INVALID_SCHEDULING_ERROR "invalid scheduling error %u"
#define FD_BAM_ERR_PREFIX_INVALID_SCHEDULING    "invalid scheduling error "

/* FD_BAM_MAX_PENDING_RESULTS is the bundle result queue depth, so long disconnects
 * don't drop SchedulerMessage payloads. */
#define FD_BAM_MAX_PENDING_RESULTS 2048U
#define FD_BAM_BUNDLE_ERR_NONE            (0U)
#define FD_BAM_BUNDLE_ERR_DESER           (1U)
#define FD_BAM_BUNDLE_ERR_GENERIC_INVALID (2U)

#define FD_BAM_ERR_GENERIC_INVALID_NONE                     (0U)
#define FD_BAM_ERR_GENERIC_INVALID_BUILDER_INFO_UNAVAILABLE (1U)

#define FD_BAM_ERR_GENERIC_INVALID_CNT (sizeof(FD_BAM_ERR_GENERIC_INVALID_STRINGS)/sizeof(FD_BAM_ERR_GENERIC_INVALID_STRINGS[0]))
static char const * const FD_BAM_ERR_GENERIC_INVALID_STRINGS[] = {
  [ FD_BAM_ERR_GENERIC_INVALID_NONE                     ] = NULL,
  [ FD_BAM_ERR_GENERIC_INVALID_BUILDER_INFO_UNAVAILABLE ] = FD_BAM_ERR_MSG_BUILDER_INFO_UNAVAILABLE,
};

#define FD_BAM_SHRED_SOCK_MAX          8UL

typedef struct {
  uint  seq_id;            /* Uniquely assigned for a single leader rotation. 0 is valid seq_id. UINT_MAX is never produced. */
  ulong slot;              /* Slot associated with the batch. Executed bundles use the bank/Poh slot. 0 means the scheduler supplied no slot hint. */
  uchar bundle_txn_cnt;    /* Declared transaction count from the scheduler, capped to FD_PACK_MAX_TXN_PER_BUNDLE. Also the number of per-transaction result entries populated below; consumers must only examine indices [0,bundle_txn_cnt). */
  _Bool execution_success; /* Batch committed flag. Set to true only when the whole batch is committed. Note: individual committed transactions can still have execution_success=false (fees-only / instruction error), encoded via TransactionCommittedResult.execution_success. */
  ushort scheduling_error; /* bam_types_SchedulingError reason code when the batch never scheduled; FD_BAM_SCHED_ERR_NONE when scheduling succeeded or the bundle executed. */
  bam_types_TransactionErrorReason transaction_err[ FD_PACK_MAX_TXN_PER_BUNDLE ]; /* Per-transaction bam_types_TransactionErrorReason for indices <bundle_txn_cnt. */
  uchar transaction_err_count; /* Number of transaction errors. 0 denotes success. */
  uint  consumed_cus    [ FD_PACK_MAX_TXN_PER_BUNDLE ]; /* Actual compute units consumed per transaction (exec+account data), even when the bundle later reverts. 0 when the txn never executed. */
  ulong feepayer_balance_lamports[ FD_PACK_MAX_TXN_PER_BUNDLE ]; /* Fee payer post-balance per transaction. Only meaningful for committed results. */
  uint  loaded_accounts_data_size[ FD_PACK_MAX_TXN_PER_BUNDLE ]; /* Loaded accounts data size (bytes) per transaction. Only meaningful for committed results. */
  _Bool sanitize_success[ FD_PACK_MAX_TXN_PER_BUNDLE ];  /* Boolean sanitize outcome per transaction (true=passed bank sanitize, false=failed). When false, transaction_err typically reports SANITIZE_FAILURE. */
  uchar bundle_err;        /* FD_BAM_BUNDLE_ERR_* selector for bundle-level rejection prior to execution. */
  uchar deser_index;       /* Zero-based transaction index tied to the deserialization error; only valid when bundle_err==FD_BAM_BUNDLE_ERR_DESER. */
  uchar deser_reason;      /* bam_types_DeserializationErrorReason enumerator for the failure reported by deser_index; only meaningful when bundle_err==FD_BAM_BUNDLE_ERR_DESER. */
  uchar generic_invalid_reason; /* FD_BAM_ERR_GENERIC_INVALID_* describing a generic invalid rejection; only meaningful when bundle_err==FD_BAM_BUNDLE_ERR_GENERIC_INVALID. */
  uchar generic_invalid_index;  /* Optional index tied to the generic invalid rejection (e.g. pack idx when pack rejects a bundle). */
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

#define FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE    (1UL<<0)
#define FD_BAM_STATUS_FSEQ_CURRENT_SLOT_HAS_BAM_WORK (1UL<<1)

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
  uchar prio_fee_recipient[ 32 ]; /* Decoded priority fee recipient pubkey */
  uint  commission_bps;           /* Validator commission expressed in basis points */
  uint  has_prio_fee_recipient;   /* Non-zero when prio_fee_recipient contains a valid pubkey */
  uint  version;                  /* Monotonically increasing update counter */
} fd_bam_fee_cfg_t;

#define FD_BAM_STEM_SIG_GOSSIP_UPDATE (0xAB) // randomly assigned
#define FD_BAM_STEM_SIG_SHRED_UPDATE  (0xAC) // randomly assigned

#define FD_BAM_SCHED_ERR_NONE            USHORT_MAX
#define FD_BAM_SCHED_ERR_POH_TIMEOUT     bam_types_SchedulingError_POH_TIMEOUT
#define FD_BAM_SCHED_ERR_OUTSIDE_SLOT    bam_types_SchedulingError_OUTSIDE_LEADER_SLOT
#define FD_BAM_SCHED_ERR_CONTAINER_FULL  bam_types_SchedulingError_CONTAINER_FULL

static inline fd_bam_bundle_result_t
fd_bam_result_base( uint  seq_id,
                    ulong slot,
                    uchar txn_cnt ) {
  return (fd_bam_bundle_result_t) {
    .seq_id           = seq_id,
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
  if( FD_LIKELY( idx<FD_PACK_MAX_TXN_PER_BUNDLE ) ) res->transaction_err[ idx ] = reason;
}

static inline void
fd_bam_result_add_txn_error( fd_bam_bundle_result_t *      res,
                             ulong                         idx,
                             bam_types_TransactionErrorReason reason ) {
  if( FD_UNLIKELY( idx>=FD_PACK_MAX_TXN_PER_BUNDLE ) ) return;
  fd_bam_result_set_txn_error( res, idx, reason );
  res->transaction_err_count++;
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
  case FD_PACK_INSERT_REJECT_INVALID_NONCE:    return bam_types_TransactionErrorReason_SIGNATURE_FAILURE;
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
  default:
    if( FD_LIKELY( err<=FD_RUNTIME_TXN_ERR_ACCOUNT_IN_USE && err>=FD_RUNTIME_TXN_ERR_COMMIT_CANCELLED ) )
      return (bam_types_TransactionErrorReason)( -err - 1 );
    return bam_types_TransactionErrorReason_COMMIT_CANCELLED;
  }
}

#endif /* HEADER_fd_src_disco_bam_fd_bam_types_h */
