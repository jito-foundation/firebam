#ifndef HEADER_fd_src_disco_bam_fd_bam_publish_h
#define HEADER_fd_src_disco_bam_fd_bam_publish_h

#include "fd_bam_types.h"          /* fd_bam_bundle_result_t */
#include "../fd_txn_m.h"           /* FD_TXN_M_TPU_SOURCE_BAM */
#include "../pack/fd_microblock.h" /* fd_txn_p_t, FD_EXECUTED_TXN_KIND_* */
#include "../stem/fd_stem.h"       /* fd_stem_context_t, fd_stem_publish, fd_chunk_to_laddr, fd_dcache_compact_next */

/* fd_bam_publish_result copies a BAM bundle result into the next dcache
   chunk of a result output link, publishes it on the stem, and advances
   the chunk in place.  Shared by every tile that produces
   fd_bam_bundle_result_t frags so the dcache chunk-advance lives in
   exactly one place.

   Returns 1 if a frag was published.  out_idx==ULONG_MAX (result link
   not wired) is a no-op and returns 0. */

static inline _Bool
fd_bam_publish_result( fd_stem_context_t *            stem,
                       ulong                          out_idx,
                       void *                         out_mem,
                       ulong *                        out_chunk,
                       ulong                          out_chunk0,
                       ulong                          out_wmark,
                       fd_bam_bundle_result_t const * res ) {
  if( FD_UNLIKELY( out_idx==ULONG_MAX ) ) return 0;
  fd_bam_bundle_result_t * out = fd_chunk_to_laddr( out_mem, *out_chunk );
  *out = *res;
  fd_stem_publish( stem, out_idx, 0UL, *out_chunk, sizeof(fd_bam_bundle_result_t), 0UL, 0UL,
                   fd_frag_meta_ts_comp( fd_tickcount() ) );
  *out_chunk = fd_dcache_compact_next( *out_chunk, sizeof(fd_bam_bundle_result_t), out_chunk0, out_wmark );
  return 1;
}

/* fd_bam_publish_txn_completions publishes the first signature of each
   BAM transaction in a microblock PoH accepted on the executed_txn
   link, with sig FD_EXECUTED_TXN_KIND_LANDED if it executed
   successfully and FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED
   otherwise.  Pack uses these to retire BAM work.  Other sources are
   skipped; pack learns of them as upstream does.  out_idx==ULONG_MAX
   (link not wired, BAM disabled) is a no-op. */

static inline void
fd_bam_publish_txn_completions( fd_stem_context_t * stem,
                                ulong               out_idx,
                                void *              out_mem,
                                ulong *             out_chunk,
                                ulong               out_chunk0,
                                ulong               out_wmark,
                                fd_txn_p_t const *  txns,
                                ulong               txn_cnt ) {
  if( FD_UNLIKELY( out_idx==ULONG_MAX ) ) return;
  for( ulong i=0UL; i<txn_cnt; i++ ) {
    if( FD_UNLIKELY( txns[ i ].source_tpu!=FD_TXN_M_TPU_SOURCE_BAM ) ) continue;
    ulong event_kind = (txns[ i ].flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS) ? FD_EXECUTED_TXN_KIND_LANDED : FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED;
    fd_memcpy( fd_chunk_to_laddr( out_mem, *out_chunk ),
               fd_txn_get_signatures( TXN(txns+i), txns[ i ].payload ), FD_TXN_SIGNATURE_SZ );
    fd_stem_publish( stem, out_idx, event_kind, *out_chunk, FD_TXN_SIGNATURE_SZ, 0UL, 0UL, fd_frag_meta_ts_comp( fd_tickcount() ) );
    *out_chunk = fd_dcache_compact_next( *out_chunk, FD_TXN_SIGNATURE_SZ, out_chunk0, out_wmark );
  }
}

#endif /* HEADER_fd_src_disco_bam_fd_bam_publish_h */
