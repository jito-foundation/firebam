#ifndef HEADER_fd_src_disco_bam_fuzz_bam_e2e_dedup_h
#define HEADER_fd_src_disco_bam_fuzz_bam_e2e_dedup_h

#include "fd_bam_tile.h"
#include "fuzz_bam_e2e_links.h"
#include "../fd_txn_m.h"

typedef struct bam_fuzz_dedup bam_fuzz_dedup_t;

typedef struct {
  ulong dedup_before;
  ulong dedup_after;
} bam_fuzz_dedup_result_t;

bam_fuzz_dedup_t *
bam_fuzz_dedup_new( fd_wksp_t * wksp,
                    fd_wksp_t * in_mem,
                    ulong       in_chunk0,
                    ulong       in_wmark,
                    bam_fuzz_link_t * out );

void
bam_fuzz_dedup_delete( bam_fuzz_dedup_t * h );

bam_fuzz_dedup_result_t
bam_fuzz_dedup_frag( bam_fuzz_dedup_t * h,
                     fd_frag_meta_t const * meta,
                     ulong seq );

#endif /* HEADER_fd_src_disco_bam_fuzz_bam_e2e_dedup_h */
