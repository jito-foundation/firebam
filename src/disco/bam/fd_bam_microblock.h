#ifndef HEADER_fd_src_disco_bam_fd_bam_microblock_h
#define HEADER_fd_src_disco_bam_fd_bam_microblock_h

#include "../tiles.h"       /* fd_microblock_trailer_t, MAX_MICROBLOCK_SZ */
#include "fd_bam_types.h"   /* fd_bam_bundle_result_t */

/* Successful BAM execution feedback is provisional until PoH accepts the
   corresponding microblock.  Such feedback is carried on the internal
   execle/bank -> PoH link immediately before the ordinary trailer:

     [ fd_txn_p_t ... ][ optional fd_bam_bundle_result_t ][ trailer ]

   fd_bam_microblock_parse distinguishes the two layouts solely by their
   validated size. */

typedef struct {
  ulong                           txn_cnt;
  fd_bam_bundle_result_t const *  result;  /* NULL for an ordinary microblock */
  fd_microblock_trailer_t const * trailer;
} fd_bam_microblock_view_t;

/* Successful execution is provisional until PoH accepts the carrying
   microblock.  Producers publish non-provisional failures immediately,
   but attach provisional success to the microblock for PoH to resolve. */

FD_FN_PURE static inline _Bool
fd_bam_result_is_provisional( fd_bam_bundle_result_t const * result ) {
  return !!result->execution_success;
}

static inline void
fd_bam_result_resolve_at_poh( fd_bam_bundle_result_t * result,
                              int                      accepted ) {
  if( FD_LIKELY( accepted ) ) return;
  result->execution_success = 0U;
  result->scheduling_error  = FD_BAM_SCHED_ERR_POH_TIMEOUT;
}

FD_STATIC_ASSERT( sizeof(fd_bam_bundle_result_t)%sizeof(fd_txn_p_t)!=0UL,
                  bam_microblock_result_size_is_unambiguous );
FD_STATIC_ASSERT( MAX_TXN_PER_MICROBLOCK*sizeof(fd_txn_p_t) +
                  sizeof(fd_bam_bundle_result_t) +
                  sizeof(fd_microblock_trailer_t) <= MAX_MICROBLOCK_SZ,
                  bam_result_microblock_fits_mtu );

FD_FN_CONST static inline ulong
fd_bam_microblock_footprint( ulong txn_cnt,
                             int   has_result ) {
  return txn_cnt*sizeof(fd_txn_p_t) +
         (has_result ? sizeof(fd_bam_bundle_result_t) : 0UL) +
         sizeof(fd_microblock_trailer_t);
}

static inline fd_microblock_trailer_t *
fd_bam_microblock_prepare_trailer( void *                         base,
                                   ulong                          txn_cnt,
                                   fd_bam_bundle_result_t const * result ) {
  uchar * trailer = (uchar *)base + txn_cnt*sizeof(fd_txn_p_t);
  if( FD_UNLIKELY( result ) ) {
    fd_memcpy( trailer, result, sizeof(fd_bam_bundle_result_t) );
    trailer += sizeof(fd_bam_bundle_result_t);
  }
  return (fd_microblock_trailer_t *)trailer;
}

static inline int
fd_bam_microblock_parse( void const *               data,
                         ulong                      sz,
                         fd_bam_microblock_view_t * out ) {
  if( FD_UNLIKELY( sz<sizeof(fd_microblock_trailer_t) ) ) return 0;

  ulong body_sz = sz - sizeof(fd_microblock_trailer_t);
  int has_result = !!(body_sz%sizeof(fd_txn_p_t));
  if( FD_UNLIKELY( has_result ) ) {
    if( FD_UNLIKELY( body_sz<sizeof(fd_bam_bundle_result_t) ) ) return 0;
    body_sz -= sizeof(fd_bam_bundle_result_t);
    if( FD_UNLIKELY( body_sz%sizeof(fd_txn_p_t) ) ) return 0;
  }

  ulong txn_cnt = body_sz/sizeof(fd_txn_p_t);
  if( FD_UNLIKELY( txn_cnt>MAX_TXN_PER_MICROBLOCK || (has_result && !txn_cnt) ) ) return 0;

  uchar const * bytes = (uchar const *)data;
  *out = (fd_bam_microblock_view_t) {
    .txn_cnt = txn_cnt,
    .result  = has_result ? (fd_bam_bundle_result_t const *)(bytes + txn_cnt*sizeof(fd_txn_p_t)) : NULL,
    .trailer = (fd_microblock_trailer_t const *)(bytes + sz-sizeof(fd_microblock_trailer_t)),
  };
  return 1;
}

#endif /* HEADER_fd_src_disco_bam_fd_bam_microblock_h */
