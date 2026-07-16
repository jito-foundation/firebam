#ifndef HEADER_fd_src_disco_bam_fuzz_bam_pipeline_stage_resolv_h
#define HEADER_fd_src_disco_bam_fuzz_bam_pipeline_stage_resolv_h

#include "fd_bam_tile.h"
#include "fuzz_bam_pipeline_links.h"
#include "../fd_txn_m.h"
#include "../../flamenco/accdb/fd_accdb.h"
#include "../../flamenco/runtime/fd_bank.h"

typedef struct bam_fuzz_resolv bam_fuzz_resolv_t;

typedef struct {
  ulong pack_before;
  ulong pack_after;
  ulong bank_bam_before;
  ulong bank_bam_after;
} bam_fuzz_resolv_result_t;

bam_fuzz_resolv_t *
bam_fuzz_resolv_new( fd_wksp_t * wksp,
                     fd_wksp_t * in_mem,
                     ulong       in_chunk0,
                     ulong       in_wmark,
                     bam_fuzz_link_t * pack_out,
                     bam_fuzz_link_t * bank_bam_out );

void
bam_fuzz_resolv_delete( bam_fuzz_resolv_t * h );

void
bam_fuzz_resolv_prepare_slot( bam_fuzz_resolv_t * h,
                              ulong               slot,
                              fd_banks_t *        banks,
                              fd_accdb_t *        accdb,
                              fd_bank_t *         bank );

bam_fuzz_resolv_result_t
bam_fuzz_resolv_frag( bam_fuzz_resolv_t * h,
                      fd_frag_meta_t const * meta,
                      ulong seq );

bam_fuzz_resolv_result_t
bam_fuzz_resolv_credit( bam_fuzz_resolv_t * h );

#endif /* HEADER_fd_src_disco_bam_fuzz_bam_pipeline_stage_resolv_h */
