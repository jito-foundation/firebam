#ifndef HEADER_fd_src_disco_bam_fuzz_bam_e2e_verify_h
#define HEADER_fd_src_disco_bam_fuzz_bam_e2e_verify_h

#include "fd_bam_tile.h"
#include "fuzz_bam_e2e_links.h"
#include "../fd_txn_m.h"

typedef struct bam_fuzz_verify bam_fuzz_verify_t;

typedef struct {
  ulong verify_before;
  ulong verify_after;
  ulong bank_bam_before;
  ulong bank_bam_after;
} bam_fuzz_verify_result_t;

bam_fuzz_verify_t *
bam_fuzz_verify_new( fd_wksp_t * wksp,
                     fd_wksp_t * in_mem,
                     ulong       in_chunk0,
                     ulong       in_wmark,
                     bam_fuzz_link_t * out,
                     bam_fuzz_link_t * bank_bam_out );

void
bam_fuzz_verify_delete( bam_fuzz_verify_t * h );

bam_fuzz_verify_result_t
bam_fuzz_verify_frag( bam_fuzz_verify_t * h,
                      fd_frag_meta_t const * meta,
                      ulong seq );

#endif /* HEADER_fd_src_disco_bam_fuzz_bam_e2e_verify_h */
