#ifndef HEADER_fd_src_app_shared_fd_config_bam_h
#define HEADER_fd_src_app_shared_fd_config_bam_h

/* FireBAM: the [development.bam] and [tiles.bam] options, fd_config_t's
   bam member.  It is the last member of fd_config_t so every upstream
   member keeps its upstream offset.  The member paths
   (bam.development.X, bam.tiles.X) differ from the TOML keys
   (development.bam.X, tiles.bam.X), so fd_config_parse.c pops them with
   CFG_POP1. */

#include "../../waltz/http/fd_url.h"

struct fd_config_bam {
  struct {
    char ssl_key_log_file[ PATH_MAX ];
    uint buffer_size_kib;
    int  dump_bam_txns;
    int  dump_bam_slot_first_txn;
  } development;

  struct {
    int   enabled;
    char  url[ FD_URL_MAX ];
    char  tls_domain_name[ FD_SNI_BUF_MAX ];
    ulong keepalive_interval_millis;
    int   tls_cert_verify;
  } tiles;
};

typedef struct fd_config_bam fd_config_bam_t;

#endif /* HEADER_fd_src_app_shared_fd_config_bam_h */
