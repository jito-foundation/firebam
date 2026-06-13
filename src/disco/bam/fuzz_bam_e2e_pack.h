#ifndef HEADER_fd_src_disco_bam_fuzz_bam_e2e_pack_h
#define HEADER_fd_src_disco_bam_fuzz_bam_e2e_pack_h

#include "fd_bam_tile.h"
#include "fuzz_bam_e2e_links.h"
#include "../fd_txn_m.h"

typedef struct bam_fuzz_pack bam_fuzz_pack_t;

typedef struct {
  ulong execle_before;
  ulong execle_after;
  ulong poh_before;
  ulong poh_after;
  ulong bam_leader_before;
  ulong bam_leader_after;
  ulong bam_result_before;
  ulong bam_result_after;
  ulong pending_work_cnt;
  ulong scheduled_work_cnt;
} bam_fuzz_pack_result_t;

bam_fuzz_pack_t *
bam_fuzz_pack_new( fd_wksp_t * wksp,
                   fd_wksp_t * in_mem,
                   ulong       in_chunk0,
                   ulong       in_wmark,
                   bam_fuzz_link_t * execle_out,
                   bam_fuzz_link_t * bam_leader_out,
                   bam_fuzz_link_t * bam_result_out,
                   ulong **          execle_busy_fseq );

void
bam_fuzz_pack_delete( bam_fuzz_pack_t * h );

bam_fuzz_pack_result_t
bam_fuzz_pack_set_leader_slot( bam_fuzz_pack_t * h,
                               ulong             slot,
                               void const *      bank,
                               ulong             bank_idx,
                               long              now_ns );

bam_fuzz_pack_result_t
bam_fuzz_pack_frag( bam_fuzz_pack_t *    h,
                    fd_frag_meta_t const * meta,
                    ulong                seq );

bam_fuzz_pack_result_t
bam_fuzz_pack_credit( bam_fuzz_pack_t * h );

#endif /* HEADER_fd_src_disco_bam_fuzz_bam_e2e_pack_h */
