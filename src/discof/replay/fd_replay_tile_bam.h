#ifndef HEADER_fd_src_discof_replay_fd_replay_tile_bam_h
#define HEADER_fd_src_discof_replay_fd_replay_tile_bam_h

/* FireBAM fields of fd_replay_tile_t (REPLAY_TILE_BAM_CTX), at its end
   so the upstream fields keep their offsets.  Included once by
   fd_replay_tile_private.h.  The BAM functions are in
   fd_replay_tile_bam.c, which fd_replay_tile.c includes. */

#define REPLAY_TILE_BAM_CTX int bam_enabled; /* startup topology includes BAM */

#endif /* HEADER_fd_src_discof_replay_fd_replay_tile_bam_h */
