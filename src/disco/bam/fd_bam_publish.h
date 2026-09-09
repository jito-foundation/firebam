#ifndef HEADER_fd_src_disco_bam_fd_bam_publish_h
#define HEADER_fd_src_disco_bam_fd_bam_publish_h

#include "fd_bam_types.h"    /* fd_bam_bundle_result_t */
#include "../stem/fd_stem.h" /* fd_stem_context_t, fd_stem_publish, fd_chunk_to_laddr, fd_dcache_compact_next */

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

#endif /* HEADER_fd_src_disco_bam_fd_bam_publish_h */
