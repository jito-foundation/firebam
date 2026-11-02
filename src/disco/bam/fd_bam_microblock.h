#ifndef HEADER_fd_src_disco_bam_fd_bam_microblock_h
#define HEADER_fd_src_disco_bam_fd_bam_microblock_h

/* BAM result feedback helpers for the execution, motor and pack tiles. */

#include "../tiles.h"       /* fd_microblock_trailer_t, MAX_MICROBLOCK_SZ, fd_stem_publish */
#include "fd_bam_types.h"   /* fd_bam_bundle_result_t */

/* Execle attaches the result of a BAM batch it executed to the batch's
   last microblock, so the result reaches the BAM tile only after motor
   handles that microblock.  The result rides the internal execle ->
   motor link right after the ordinary trailer:

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

/* fd_bam_microblock_attach_result appends the result res to the
   one-transaction microblock whose trailer is at trailer, and returns
   the new microblock size. */

static inline ulong
fd_bam_microblock_attach_result( fd_microblock_trailer_t *      trailer,
                                 fd_bam_bundle_result_t const * res ) {
  fd_memcpy( trailer+1, res, sizeof(fd_bam_bundle_result_t) );
  return fd_bam_microblock_footprint( 1UL, 1 );
}

/* fd_bam_microblock_strip_result returns the result the
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

/* fd_bam_publish_dropped_result publishes the result the microblock at
   data carries, if any, when motor drops the microblock instead of
   recording it.  The microblock's slot has ended, so as jito-solana does
   for work whose slot expired, the batch is reported not committed with
   a POH_TIMEOUT scheduling error, BAM's catch-all for work PoH did not
   record, whatever its execution outcome. */

static inline void
fd_bam_publish_dropped_result( fd_stem_context_t * stem,
                               ulong               out_idx,
                               void *              out_mem,
                               ulong *             out_chunk,
                               ulong               out_chunk0,
                               ulong               out_wmark,
                               void const *        data,
                               ulong               sz ) {
  if( FD_UNLIKELY( out_idx==ULONG_MAX ) ) return;
  fd_bam_bundle_result_t const * carried = fd_bam_microblock_strip_result( data, &sz );
  if( FD_LIKELY( !carried ) ) return;
  fd_bam_bundle_result_t res = fd_bam_result_base( carried->seq_id, carried->scheduler_gen, carried->slot, carried->bundle_txn_cnt );
  res.scheduling_error = FD_BAM_SCHED_ERR_POH_TIMEOUT;
  FD_LOG_WARNING(( "motor dropped the microblock carrying the result of BAM batch seq_id=%u slot=%lu, reporting POH_TIMEOUT",
                   res.seq_id, res.slot ));
  fd_bam_publish_result( stem, out_idx, out_mem, out_chunk, out_chunk0, out_wmark, &res );
}

#endif /* HEADER_fd_src_disco_bam_fd_bam_microblock_h */
