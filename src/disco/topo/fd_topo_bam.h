#ifndef HEADER_fd_src_disco_topo_fd_topo_bam_h
#define HEADER_fd_src_disco_topo_fd_topo_bam_h

/* FireBAM: configuration of the bam tile, fd_topo_tile_t's bam member. */

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

#endif /* HEADER_fd_src_disco_topo_fd_topo_bam_h */
