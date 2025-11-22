#ifndef HEADER_fd_src_disco_bam_fd_bam_types_h
#define HEADER_fd_src_disco_bam_fd_bam_types_h

#include "../pack/fd_pack.h" /* FD_PACK_MAX_TXN_PER_BUNDLE */
#include "proto/bam_types.pb.h"
#include "../../util/net/fd_net_headers.h"

/* FD_BAM_MAX_PENDING_RESULTS is the bundle result queue depth, so long disconnects
 * don't drop SchedulerMessage payloads. */
#define FD_BAM_MAX_PENDING_RESULTS 2048U
#define FD_BAM_GENERIC_INVALID_MSG_MAX 96U

#define FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT ULONG_MAX

typedef struct {
  uint  seq_id;    /* Uniquely assigned for a single leader rotation. 0 is valid seq_id. UINT_MAX is never produced. */
  ulong slot;         /* Slot associated with the batch. Executed bundles use the bank/Poh slot, while pre-execution drops mirror AtomicTxnBatch.max_schedule_slot. 0 means the scheduler supplied no slot hint. */
  uchar bundle_txn_cnt; /* Declared transaction count from the scheduler, capped to FD_PACK_MAX_TXN_PER_BUNDLE. 0 until pack/bank has observed bundle metadata. */
  uchar txn_cnt;         /* Number of per-transaction result entries populated below. Consumers must only examine indices [0,txn_cnt); value saturates at FD_PACK_MAX_TXN_PER_BUNDLE. */
  uchar execution_success; /* Bundle-level success flag. Set to 1 only when every transaction executed and committed; remains 0 for sanitize failures, revert_on_error cascades, or scheduler drops. */
  ushort scheduling_error;  /* bam_types_SchedulingError reason code when the batch never scheduled; FD_BAM_SCHED_ERR_NONE (UINT_MAX) when scheduling succeeded or the bundle executed. */
  int transaction_err[ FD_PACK_MAX_TXN_PER_BUNDLE ]; /* Per-transaction bam_types_TransactionErrorReason for indices <txn_cnt. 0 denotes success. */
  uint   consumed_cus    [ FD_PACK_MAX_TXN_PER_BUNDLE ]; /* Actual compute units consumed per transaction (exec+account data), even when the bundle later reverts. 0 when the txn never executed. */
  uchar sanitize_success[ FD_PACK_MAX_TXN_PER_BUNDLE ];  /* Boolean sanitize outcome per transaction (1=passed bank sanitize, 0=failed). When 0, transaction_err typically reports SANITIZE_FAILURE. */
  uchar has_deser_error;   /* Batch-level flag indicating deserialization or flag validation failed before execution; when set, per-transaction arrays are undefined and deser_* identify the offender. */
  uchar deser_index;       /* Zero-based transaction index tied to the deserialization error; only valid when has_deser_error==1. */
  uchar deser_reason;      /* bam_types_DeserializationErrorReason enumerator for the failure reported by deser_index; only meaningful when has_deser_error==1. */
  uchar has_generic_invalid; /* Batch-level rejection flag for generic invalid conditions (mixed modes, oversize, etc.). When 1, generic_invalid_msg contains a human-readable explanation. */
  char  generic_invalid_msg[ FD_BAM_GENERIC_INVALID_MSG_MAX ]; /* NUL-terminated ASCII detail describing a generic invalid rejection. Truncated to FD_BAM_GENERIC_INVALID_MSG_MAX-1 bytes when present. */
} fd_bam_bundle_result_t;

typedef struct {
  ulong slot;
  uint  tick;
  uint  slot_cu_budget_remaining;
} fd_bam_leader_state_t;

typedef struct {
  fd_ip4_port_t tpu_addr;      /* TPU socket advertised by BAM */
  fd_ip4_port_t tpu_fwd_addr;  /* TPU fwd socket advertised by BAM. */
  uint          use_bam;       /* `FD_BAM_CONTACT_USE_*` selector. Non-zero when BAM overrides contact info */
} fd_bam_contact_update_t;

typedef struct {
  uchar prio_fee_recipient[ 32 ]; /* Decoded priority fee recipient pubkey */
  uint  commission_bps;           /* Validator commission expressed in basis points */
  uint  has_prio_fee_recipient;   /* Non-zero when prio_fee_recipient contains a valid pubkey */
  uint  version;                  /* Monotonically increasing update counter */
} fd_bam_fee_cfg_t;

#define FD_BAM_CONTACT_USE_DEFAULT (0)
#define FD_BAM_CONTACT_USE_BAM     (1)

#define FD_BAM_STEM_SIG_GOSSIP_UPDATE (5)

#define FD_BAM_SCHED_ERR_NONE            USHORT_MAX
#define FD_BAM_SCHED_ERR_POH_TIMEOUT     bam_types_SchedulingError_POH_TIMEOUT
#define FD_BAM_SCHED_ERR_OUTSIDE_SLOT    bam_types_SchedulingError_OUTSIDE_LEADER_SLOT
#define FD_BAM_SCHED_ERR_CONTAINER_FULL  bam_types_SchedulingError_CONTAINER_FULL

#endif /* HEADER_fd_src_disco_bam_fd_bam_types_h */
