#ifndef HEADER_fd_src_disco_topo_fd_topo_bam_h
#define HEADER_fd_src_disco_topo_fd_topo_bam_h

/* FireBAM: configuration of the bam tile, fd_topo_tile_t's bam member,
   and the bam tile's core sharing rule for the auto layout. */

#include "../../waltz/http/fd_url.h"
#include "../../util/net/fd_net_headers.h"

struct fd_topo_tile_bam {
  char  url[ FD_URL_MAX ];
  char  sni[ FD_SNI_BUF_MAX ];
  fd_ip4_port_t configured_default_tpu;
  char  identity_key_path[ PATH_MAX ];
  char  key_log_path[ PATH_MAX ];
  ulong buf_sz;
  ulong keepalive_interval_nanos;
  uchar tls_cert_verify : 1;
  uchar dump_bam_mode   : 2;
};
typedef struct fd_topo_tile_bam fd_topo_tile_bam_t;

/* fd_topo_bam_smt_pair returns 1 if the tiles named a and b are the
   bundle and bam tiles, in either order, else 0.  When the auto layout
   shares cores (smt_plan in fd_topob_layout.c), it pairs them like two
   tiles of the same kind: the bundle tile has no work while BAM is
   active (fd_bundle_tile_bam.h), so bam's sibling only polls. */

FD_FN_PURE static inline int
fd_topo_bam_smt_pair( char const * a,
                      char const * b ) {
  return ( !strcmp( a, "bundle" ) && !strcmp( b, "bam"    ) ) ||
         ( !strcmp( a, "bam"    ) && !strcmp( b, "bundle" ) );
}

#endif /* HEADER_fd_src_disco_topo_fd_topo_bam_h */
