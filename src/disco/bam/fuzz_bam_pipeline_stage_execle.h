#ifndef HEADER_fd_src_disco_bam_fuzz_bam_pipeline_stage_execle_h
#define HEADER_fd_src_disco_bam_fuzz_bam_pipeline_stage_execle_h

#include "fd_bam_tile.h"
#include "fuzz_bam_pipeline_links.h"
#include "../fd_txn_m.h"
#include "../../flamenco/accdb/fd_accdb.h"
#include "../../flamenco/runtime/fd_bank.h"

typedef struct bam_fuzz_execle bam_fuzz_execle_t;

typedef struct {
  ulong poh_before;
  ulong poh_after;
  ulong bank_bam_before;
  ulong bank_bam_after;
} bam_fuzz_execle_result_t;

bam_fuzz_execle_t *
bam_fuzz_execle_new( fd_wksp_t * wksp,
                     fd_wksp_t * in_mem,
                     ulong       in_chunk0,
                     ulong       in_wmark,
                     ulong *     busy_fseq,
                     bam_fuzz_link_t * bank_bam_out );

void
bam_fuzz_execle_delete( bam_fuzz_execle_t * h );

void
bam_fuzz_execle_prepare_slot( bam_fuzz_execle_t * h,
                              ulong               slot,
                              fd_banks_t **       banks,
                              fd_accdb_t **        accdb,
                              void const **       bank,
                              ulong *             bank_idx );

bam_fuzz_execle_result_t
bam_fuzz_execle_frag( bam_fuzz_execle_t *    h,
                      fd_frag_meta_t const * meta,
                      ulong                  seq );

#endif /* HEADER_fd_src_disco_bam_fuzz_bam_pipeline_stage_execle_h */
