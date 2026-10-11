#ifndef HEADER_fd_src_disco_bam_fd_bam_microblock_h
#define HEADER_fd_src_disco_bam_fd_bam_microblock_h

/* BAM result feedback helpers for the execution, PoH and pack tiles. */

#include "../tiles.h"       /* fd_microblock_trailer_t, MAX_MICROBLOCK_SZ, fd_stem_publish */
#include "../fd_txn_m.h"    /* FD_TXN_M_TPU_SOURCE_BAM */
#include "fd_bam_types.h"   /* fd_bam_bundle_result_t */

/* Producers publish BAM failures immediately, but successful execution
   feedback is provisional until PoH accepts the corresponding
   microblock.  Such feedback is carried on the internal execle -> PoH
   link right after the ordinary trailer:

     [ fd_txn_p_t ... ][ trailer ][ optional fd_bam_bundle_result_t ]

   Only a one-transaction microblock carries a result: a non-revert BAM
   batch is a single transaction and execle publishes each atomic batch
   member in its own microblock.
   fd_bam_microblock_strip_result distinguishes the two layouts solely by
   their size, so a reader strips the result and then reads the ordinary
   microblock as upstream does. */

FD_STATIC_ASSERT( sizeof(fd_bam_bundle_result_t)%sizeof(fd_txn_p_t)!=0UL,
                  bam_microblock_result_size_is_unambiguous );
FD_STATIC_ASSERT( MAX_TXN_PER_MICROBLOCK*sizeof(fd_txn_p_t) +
                  sizeof(fd_bam_bundle_result_t) +
                  sizeof(fd_microblock_trailer_t) <= MAX_MICROBLOCK_SZ,
                  bam_result_microblock_fits_mtu );

FD_STATIC_ASSERT( sizeof(fd_txn_p_t) +
                  sizeof(fd_bam_bundle_result_t) +
                  sizeof(fd_microblock_trailer_t) <= FD_EXECLE_POH_MTU,
                  bam_result_microblock_fits_execle_poh_link );

FD_FN_CONST static inline ulong
fd_bam_microblock_footprint( ulong txn_cnt,
                             int   has_result ) {
  return txn_cnt*sizeof(fd_txn_p_t) +
         (has_result ? sizeof(fd_bam_bundle_result_t) : 0UL) +
         sizeof(fd_microblock_trailer_t);
}

/* fd_bam_microblock_attach_result appends the provisional successful
   result res to the one-transaction microblock whose trailer is at
   trailer, and returns the new microblock size. */

static inline ulong
fd_bam_microblock_attach_result( fd_microblock_trailer_t *      trailer,
                                 fd_bam_bundle_result_t const * res ) {
  fd_memcpy( trailer+1, res, sizeof(fd_bam_bundle_result_t) );
  return fd_bam_microblock_footprint( 1UL, 1 );
}

/* fd_bam_microblock_strip_result returns the provisional result the
   *sz byte microblock at data carries and removes it from *sz, or
   returns NULL if it carries none: a microblock of any other size is
   read as an ordinary one. */

static inline fd_bam_bundle_result_t const *
fd_bam_microblock_strip_result( void const * data,
                                ulong *      sz ) {
  if( FD_LIKELY( *sz!=fd_bam_microblock_footprint( 1UL, 1 ) ) ) return NULL;
  *sz = fd_bam_microblock_footprint( 1UL, 0 );
  return (fd_bam_bundle_result_t const *)( (uchar const *)data + *sz );
}

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

/* executed_txn link event kinds, carried in the mcache sig field */
#define FD_EXECUTED_TXN_KIND_LANDED                  (0UL)
#define FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED  (1UL)

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

#endif /* HEADER_fd_src_disco_bam_fd_bam_microblock_h */
