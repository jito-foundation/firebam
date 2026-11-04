#ifndef HEADER_fd_src_disco_bam_fd_bam_webpki_roots_h
#define HEADER_fd_src_disco_bam_fd_bam_webpki_roots_h

/* Mozilla's TLS server roots as compiled into jito-solana through
   webpki-roots.  Regenerate with gen_webpki_roots.py when jito-solana's
   Cargo.lock moves the webpki-roots pin. */

#include "../../util/fd_util_base.h"

FD_PROTOTYPES_BEGIN

/* One PEM certificate per entry. */
extern char const * const fd_bam_webpki_roots[];
extern ulong const        fd_bam_webpki_roots_cnt;
extern char const         fd_bam_webpki_roots_ver[];

FD_PROTOTYPES_END

#endif /* HEADER_fd_src_disco_bam_fd_bam_webpki_roots_h */
