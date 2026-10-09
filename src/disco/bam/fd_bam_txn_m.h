#ifndef HEADER_fd_src_disco_bam_fd_bam_txn_m_h
#define HEADER_fd_src_disco_bam_fd_bam_txn_m_h

/* FireBAM helpers on fd_txn_m_t, whose BAM metadata overlays the Block
   Engine fields.  Included at the end of fd_txn_m.h. */

#include "../fd_txn_m.h"
#include <stddef.h> /* offsetof */

FD_STATIC_ASSERT( sizeof(fd_txn_m_t)==88UL, fd_txn_m_layout );

/* BAM ingress writes bundle_id and bundle_txn_cnt through fd_txn_m_t's
   bam union member, and every downstream tile reads them through
   block_engine, so they must stay a common initial sequence. */
#define FD_BAM_TXN_M_OVERLAY( f ) \
  ( offsetof(fd_txn_m_t, bam.f)==offsetof(fd_txn_m_t, block_engine.f) && \
    sizeof(((fd_txn_m_t *)NULL)->bam.f)==sizeof(((fd_txn_m_t *)NULL)->block_engine.f) )
FD_STATIC_ASSERT( FD_BAM_TXN_M_OVERLAY( bundle_id      ), bam_txn_m_bundle_id_overlay      );
FD_STATIC_ASSERT( FD_BAM_TXN_M_OVERLAY( bundle_txn_cnt ), bam_txn_m_bundle_txn_cnt_overlay );
#undef FD_BAM_TXN_M_OVERLAY

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
  if( FD_UNLIKELY( txnm->source_tpu==FD_TXN_M_TPU_SOURCE_BAM && group_id ) )
    group_id = (1UL<<63) | ((ulong)txnm->bam.seq_id+1UL);

  return group_id;
}

#endif /* HEADER_fd_src_disco_bam_fd_bam_txn_m_h */
