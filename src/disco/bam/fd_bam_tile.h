#ifndef HEADER_fd_src_disco_bam_fd_bam_tile_h
#define HEADER_fd_src_disco_bam_fd_bam_tile_h

#include "../topo/fd_topo.h"

struct fd_bam_tile;
typedef struct fd_bam_tile fd_bam_tile_t;

#define FD_BAM_CLIENT_REQUEST_TIMEOUT ((long)8e9) /* 8 seconds */

#define FD_BAM_AUTH_LABEL "X_OFF_CHAIN_JITO_BAM_V1\0"
#define FD_BAM_AUTH_LABEL_LEN 24

FD_PROTOTYPES_BEGIN

extern fd_topo_run_tile_t fd_tile_bam;

FD_PROTOTYPES_END

#endif /* HEADER_fd_src_disco_bam_fd_bam_tile_h */
