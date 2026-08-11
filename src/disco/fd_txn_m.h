#ifndef HEADER_fd_src_disco_fd_txn_m_h
#define HEADER_fd_src_disco_fd_txn_m_h

/* A fd_txn_m_t is a parsed meta transaction, containing not just the
   payload */

#include "../ballet/txn/fd_txn.h"

#define FD_TXN_M_TPU_SOURCE_QUIC   (1UL)
#define FD_TXN_M_TPU_SOURCE_UDP    (2UL)
#define FD_TXN_M_TPU_SOURCE_GOSSIP (3UL)
#define FD_TXN_M_TPU_SOURCE_BUNDLE (4UL)
#define FD_TXN_M_TPU_SOURCE_TXSEND (5UL)
#define FD_TXN_M_TPU_SOURCE_BAM    (6UL)

struct fd_txn_m {
  /* The computed slot that this transaction is referencing, aka. the
     slot number of the reference_blockhash.  If it could not be
     determined, this will be the current slot. */
  ulong    reference_slot;

  ushort   payload_sz;

  /* Can be computed from the txn_t but it's expensive to parse again,
     so we just store this redundantly. */
  ushort    txn_t_sz;

  /* Source tpu and IP address for this transaction.  Note that
     source_ipv4 is in big endian. */
  uint     source_ipv4;
  uchar    source_tpu;
  uint     scheduler_arrival_tspub; /* Compact local tickcount timestamp for when the scheduler-side source first published this txn. 0 means unknown. */

  /* 3 bytes of padding here */

  struct {
    /* If the transaction is part of a bundle, the bundle_id will be
       non-zero, and if this transaction is the first one in the
       bundle, bundle_txn_cnt will be non-zero.

       The pack tile can accumulate transactions from a bundle until
       it has all of them, at which point the bundle is schedulable.

       Bundles will not arrive to pack interleaved with other bundles
       (although might be interleaved with other non-bundle
       transactions), so if pack sees the bundle_id change before
       collecting all the bundle_txn_cnt transactions, it should
       abandon the bundle, as one or more of the transactions failed
       to signature verify or resolve.

       The commission and commission_pubkey fields are provided by
       the block engine, and the validator will crank the tip payment
       program with these values, if it is not using them already.
       These fields are only provided on the first transaction in a
       bundle. */
    ulong bundle_id;
    ulong bundle_txn_cnt;
    uchar commission;
    uchar commission_pubkey[ 32 ];

  } block_engine;

  struct {
    /* An 'atomic transaction batch' is a bundle of transactions that must be processed together */
    ulong  max_schedule_slot; /* Solana slot for which this bundle is valid for (inclusive). eg if we're building slot 100, and max_schedule_slot == 100, process the txn */
    uint   seq_id;            /* Unique for a single leader rotation, propagated so downstream stages can correlate execution results */
    ushort scheduler_gen;     /* BAM scheduler identity generation, propagated to discard stale in-flight results after endpoint/key changes */
    ushort ownership_gen;     /* BAM ownership generation, propagated so pack can reject work crossing a disable/disconnect boundary */
    uchar  txn_cnt;           /* How many transactions are expected in the atomic transaction batch */
    uchar  batch_idx;         /* Index of this transaction inside the atomic transaction batch */
    uchar  revert_on_error   : 1; /* If true and any transaction in the batch fails, revert everything. otherwise commit errors */
    uchar  blockhash_expired : 1; /* Set by resolv when a known blockhash is expired so pack can reject the complete batch with the correct index */
    uchar  preprocess_failed : 1; /* Set when preprocessing failed so pack can terminate the complete batch */
  } bam;

  /* There are three additional fields at the end here, which are
     variable length and not included in the size of this struct. txn_t
     and alut are only found in frags after the verify step.
  uchar          payload[ ]
  fd_txn_t       txn_t[ ]
  fd_acct_addr_t alut[ ] */
};

typedef struct fd_txn_m fd_txn_m_t;

static FD_FN_CONST inline ulong
fd_txn_m_align( void ) {
  return alignof(fd_txn_m_t);
}

static inline ulong
fd_txn_m_footprint( ulong payload_sz,
                    ulong instr_cnt,
                    ulong addr_table_lookup_cnt,
                    ulong addr_table_adtl_cnt ) {
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_txn_m_t),     sizeof(fd_txn_m_t) );
  l = FD_LAYOUT_APPEND( l, 1UL,                     payload_sz );
  l = FD_LAYOUT_APPEND( l, fd_txn_align(),          fd_txn_footprint( instr_cnt, addr_table_lookup_cnt ) );
  l = FD_LAYOUT_APPEND( l, alignof(fd_acct_addr_t), addr_table_adtl_cnt*sizeof(fd_acct_addr_t) );
  return FD_LAYOUT_FINI( l, fd_txn_m_align() );
}

static inline int
fd_txn_m_use_prepack_sig_dedup( fd_txn_m_t const * txnm ) {
  /* Early signature dedup is disabled for block-engine bundles, which
     rely on bundle-aware handling downstream, and for BAM traffic,
     which is sequenced by the BAM node and may intentionally resend a
     transaction signature. */
  return !( txnm->block_engine.bundle_id ||
            txnm->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
}

static inline ulong
fd_txn_m_failure_group_id( fd_txn_m_t const * txnm ) {
  ulong group_id = txnm->block_engine.bundle_id;

  /* BAM seq_ids and block-engine bundle_ids have independent namespaces.
     Keep all BAM batches that require peer-failure tracking in a disjoint
     namespace, including one-transaction atomic batches. */
  if( FD_UNLIKELY( txnm->source_tpu==FD_TXN_M_TPU_SOURCE_BAM &&
                   (group_id || txnm->bam.txn_cnt>1U) ) )
    group_id = (1UL<<63) | ((ulong)txnm->bam.seq_id+1UL);

  return group_id;
}

static inline uchar *
fd_txn_m_payload( fd_txn_m_t * txnm ) {
  return (uchar *)(txnm+1UL);
}

static inline uchar const *
fd_txn_m_payload_const( fd_txn_m_t const * txnm ) {
  return (uchar const *)(txnm+1UL);
}

static inline fd_txn_t *
fd_txn_m_txn_t( fd_txn_m_t * txnm ) {
  return (fd_txn_t *)fd_ulong_align_up( (ulong)(txnm+1UL) + txnm->payload_sz, alignof( fd_txn_t ) );
}

static inline fd_txn_t const *
fd_txn_m_txn_t_const( fd_txn_m_t const * txnm ) {
  return (fd_txn_t const *)fd_ulong_align_up( (ulong)(txnm+1UL) + txnm->payload_sz, alignof( fd_txn_t ) );
}

static inline fd_acct_addr_t *
fd_txn_m_alut( fd_txn_m_t * txnm ) {
  return (fd_acct_addr_t *)fd_ulong_align_up( fd_ulong_align_up( (ulong)(txnm+1UL) + txnm->payload_sz, alignof( fd_txn_t ) )+txnm->txn_t_sz, alignof( fd_acct_addr_t ) );
}

static inline ulong
fd_txn_m_realized_footprint( fd_txn_m_t const * txnm,
                             int                include_txn_t,
                             int                include_alut ) {
  if( FD_LIKELY( include_txn_t ) ) {
    ulong l = FD_LAYOUT_INIT;
    l = FD_LAYOUT_APPEND( l, alignof(fd_txn_m_t),     sizeof(fd_txn_m_t) );
    l = FD_LAYOUT_APPEND( l, 1UL,                     txnm->payload_sz );
    l = FD_LAYOUT_APPEND( l, fd_txn_align(),          fd_txn_footprint( fd_txn_m_txn_t_const( txnm )->instr_cnt, fd_txn_m_txn_t_const( txnm )->addr_table_lookup_cnt ) );
    l = FD_LAYOUT_APPEND( l, alignof(fd_acct_addr_t), fd_uchar_if(include_alut, fd_txn_m_txn_t_const( txnm )->addr_table_adtl_cnt, 0U)*sizeof(fd_acct_addr_t) );

    /* FD_LAYOUT_FINI is not included since the _realized_ footprint
       should not include the extra padding typically added after the
       last struct used to align the entire footprint. */
    return l;
  } else {
    ulong l = FD_LAYOUT_INIT;
    l = FD_LAYOUT_APPEND( l, alignof(fd_txn_m_t), sizeof(fd_txn_m_t) );
    l = FD_LAYOUT_APPEND( l, 1UL, txnm->payload_sz );
    return l;
  }
}

#define FD_TPU_RAW_MTU FD_ULONG_ALIGN_UP(                 \
                           sizeof(fd_txn_m_t)+FD_TPU_MTU, \
                           alignof(fd_txn_m_t) )

#define FD_TPU_PARSED_MTU FD_ULONG_ALIGN_UP(                    \
                              FD_ULONG_ALIGN_UP(                \
                                 sizeof(fd_txn_m_t)+FD_TPU_MTU, \
                                 alignof(fd_txn_t) )            \
                              +FD_TXN_MAX_SZ,                   \
                              alignof(fd_txn_m_t) )

#define FD_TPU_RESOLVED_MTU FD_ULONG_ALIGN_UP(                              \
                              FD_ULONG_ALIGN_UP(                            \
                                 FD_ULONG_ALIGN_UP(                         \
                                    sizeof(fd_txn_m_t)+FD_TPU_MTU,          \
                                    alignof(fd_txn_t) )                     \
                                 +FD_TXN_MAX_SZ,                            \
                                 alignof(fd_acct_addr_t) )                  \
                              +FD_TXN_ACCT_ADDR_MAX*sizeof(fd_acct_addr_t), \
                              alignof(fd_txn_m_t) )

#endif /* HEADER_fd_src_disco_fd_txn_m_h */
