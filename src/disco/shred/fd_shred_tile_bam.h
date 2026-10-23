#ifndef HEADER_fd_src_disco_shred_fd_shred_tile_bam_h
#define HEADER_fd_src_disco_shred_fd_shred_tile_bam_h

/* FireBAM fields of fd_shred_ctx_t (SHRED_TILE_BAM_CTX), at its end so
   the upstream fields keep their offsets.  Included once by
   fd_shred_tile.c.  The BAM functions are in fd_shred_tile_bam.c. */

#include "../bam/fd_bam_types.h"

#define SHRED_TILE_BAM_CTX                                     \
  uchar                    bam_dests_cnt;                      \
  fd_shred_dest_weighted_t bam_dests[ FD_BAM_SHRED_SOCK_MAX ]; \
  ulong                    bam_leader_soon_slot;               \
  int                      bam_leader_soon;                    \
  fd_bam_shred_update_t    bam_shred_upd_buf[1];

#endif /* HEADER_fd_src_disco_shred_fd_shred_tile_bam_h */
