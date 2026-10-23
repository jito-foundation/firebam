#ifndef HEADER_fd_src_disco_fd_txn_bam_h
#define HEADER_fd_src_disco_fd_txn_bam_h

/* FireBAM transaction metadata: fd_txn_m_bam_t is the bam member of
   fd_txn_m_t and fd_txn_bam_t that of fd_txn_e_t. */

#include "../util/fd_util_base.h"

/* BAM never provides a commission, so its metadata overlays fd_txn_m_t's
   block_engine fields.  bundle_id and bundle_txn_cnt are a common
   initial sequence with block_engine, which remains the spelling for
   reading them (bam/fd_bam_txn_m.h). */
struct fd_txn_m_bam {
  ulong  bundle_id;
  ulong  bundle_txn_cnt;
  /* An 'atomic transaction batch' is a bundle of transactions that must be processed together */
  ulong  max_schedule_slot; /* Solana slot for which this bundle is valid for (inclusive). eg if we're building slot 100, and max_schedule_slot == 100, process the txn */
  uint   seq_id;            /* Unique for a single leader rotation, propagated so downstream stages can correlate execution results */
  ushort scheduler_gen;     /* BAM scheduler identity generation, propagated to discard stale in-flight results after endpoint/key changes */
  ushort ownership_gen;     /* BAM ownership generation, propagated so pack can reject work crossing a disable/disconnect boundary */
  uchar  txn_cnt;           /* How many transactions are expected in the atomic transaction batch */
  uchar  batch_idx;         /* Index of this transaction inside the atomic transaction batch */
  uchar  revert_on_error   : 1; /* If true and any transaction in the batch fails, revert everything. otherwise commit errors */
  uchar                   : 1; /* Retain the former expiry flag bit for layout compatibility. */
  uchar  preprocess_failed : 1; /* Set when preprocessing failed so pack can terminate the complete batch */
};
typedef struct fd_txn_m_bam fd_txn_m_bam_t;

/* BAM execution metadata carried by pack on an fd_txn_e_t.  Valid only
   when txnp->source_tpu is FD_TXN_M_TPU_SOURCE_BAM. */
struct fd_txn_bam {
  uint   seq_id;
  ushort scheduler_gen;
  uchar  batch_idx;
  _Bool  revert_on_error;
};
typedef struct fd_txn_bam fd_txn_bam_t;

#endif /* HEADER_fd_src_disco_fd_txn_bam_h */
