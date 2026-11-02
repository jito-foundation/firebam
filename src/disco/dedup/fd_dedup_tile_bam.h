#ifndef HEADER_fd_src_disco_dedup_fd_dedup_tile_bam_h
#define HEADER_fd_src_disco_dedup_fd_dedup_tile_bam_h

/* FireBAM fields of fd_dedup_ctx_t (DEDUP_TILE_BAM_CTX), at its end so
   the upstream fields keep their offsets.  Included once by
   fd_dedup_tile.c.  The BAM functions are in fd_dedup_tile_bam.c. */

#define DEDUP_TILE_BAM_CTX                            \
  ulong bam_seq;    /* next bam_verif seq to admit */ \
  ulong verify_cnt; /* number of verify ins */

#endif /* HEADER_fd_src_disco_dedup_fd_dedup_tile_bam_h */
