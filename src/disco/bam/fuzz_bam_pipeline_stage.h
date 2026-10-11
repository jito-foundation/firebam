#ifndef HEADER_fd_src_disco_bam_fuzz_bam_pipeline_stage_h
#define HEADER_fd_src_disco_bam_fuzz_bam_pipeline_stage_h

/* Stage adapters for fuzz_bam_pipeline_stateful.  Each adapter owns one
   real tile context (verify, dedup, resolv, pack, execle) and drives its
   stem callbacks directly over synthetic links. */

#include "fd_bam_tile.h"
#include "../stem/fd_stem.h"
#include "../../flamenco/fd_flamenco_base.h"
#include "../../flamenco/accdb/fd_accdb_base.h"

typedef struct {
  fd_wksp_t *      mem;
  fd_frag_meta_t * mcache;
  ulong            chunk0;
  ulong            wmark;
  ulong            depth;
} bam_fuzz_link_t;

/* Signature-valid transaction fixtures shared by the harness and the
   execle adapter.  Index BAM_FUZZ_FIXTURE_CNT is the 4096-byte v1
   transaction. */

#define BAM_FUZZ_FIXTURE_CNT (9UL)

uchar const *
bam_fuzz_fixture( ulong   idx,
                  ulong * sz );

/* bam_fuzz_outs_t is the output side of an adapter: the links it owns
   and a stem context with unreliable outs (no credit accounting).
   in_fseq absorbs credit returns. */

#define BAM_FUZZ_OUT_MAX (4UL)

typedef struct {
  fd_stem_context_t stem[1];
  bam_fuzz_link_t   link        [ BAM_FUZZ_OUT_MAX ];
  fd_frag_meta_t *  mcaches     [ BAM_FUZZ_OUT_MAX ];
  ulong             seqs        [ BAM_FUZZ_OUT_MAX ];
  ulong             depths      [ BAM_FUZZ_OUT_MAX ];
  ulong             cr_avail    [ BAM_FUZZ_OUT_MAX ];
  int               out_reliable[ BAM_FUZZ_OUT_MAX ];
  ulong             min_cr_avail;
  ulong             in_fseq_value;
  ulong *           in_fseq[1];
} bam_fuzz_outs_t;

/* bam_fuzz_res_t gives, per out link, the seqs [lo,hi) an adapter call
   published. */

typedef struct { ulong lo, hi; } bam_fuzz_range_t;
typedef struct { bam_fuzz_range_t out[ BAM_FUZZ_OUT_MAX ]; } bam_fuzz_res_t;

/* Out link indices of the multi-output adapters */

#define BAM_FUZZ_PACK_OUT_EXECLE     (0UL)
#define BAM_FUZZ_PACK_OUT_POH        (1UL)
#define BAM_FUZZ_PACK_OUT_BAM_LEADER (2UL)
#define BAM_FUZZ_PACK_OUT_BAM_RESULT (3UL)
#define BAM_FUZZ_EXECLE_OUT_POH      (0UL)
#define BAM_FUZZ_EXECLE_OUT_BANK_BAM (1UL)

/* bam_fuzz_link_new creates an mcache and a compact dcache for depth
   frags of up to mtu bytes in wksp. */

static inline bam_fuzz_link_t *
bam_fuzz_link_new( bam_fuzz_link_t * link,
                   fd_wksp_t *       wksp,
                   ulong             depth,
                   ulong             mtu ) {
  ulong   data_sz = fd_dcache_req_data_sz( mtu, depth, 1UL, 1 );
  void *  dc_mem  = fd_wksp_alloc_laddr( wksp, fd_dcache_align(), fd_dcache_footprint( data_sz, 0UL ), 1UL );
  void *  mc_mem  = fd_wksp_alloc_laddr( wksp, fd_mcache_align(), fd_mcache_footprint( depth, 0UL ), 1UL );
  uchar * dcache  = fd_dcache_join( fd_dcache_new( dc_mem, data_sz, 0UL ) );
  FD_TEST( dcache );
  *link = (bam_fuzz_link_t) {
    .mem    = (fd_wksp_t *)dcache,
    .mcache = fd_mcache_join( fd_mcache_new( mc_mem, depth, 0UL, 0UL ) ),
    .chunk0 = fd_dcache_compact_chunk0( dcache, dcache ),
    .wmark  = fd_dcache_compact_wmark ( dcache, dcache, mtu ),
    .depth  = depth,
  };
  FD_TEST( link->mcache );
  return link;
}

/* A failed delete leaks its allocation, which the harness's per-input
   workspace leak check catches. */

static inline void
bam_fuzz_link_delete( bam_fuzz_link_t * link ) {
  fd_wksp_free_laddr( fd_mcache_delete( fd_mcache_leave( link->mcache ) ) );
  fd_wksp_free_laddr( fd_dcache_delete( fd_dcache_leave( (uchar const *)link->mem ) ) );
}

static inline bam_fuzz_link_t *
bam_fuzz_outs_add( bam_fuzz_outs_t * o,
                   ulong             idx,
                   fd_wksp_t *       wksp,
                   ulong             depth,
                   ulong             mtu ) {
  FD_TEST( idx<BAM_FUZZ_OUT_MAX );
  bam_fuzz_link_t * link = bam_fuzz_link_new( &o->link[ idx ], wksp, depth, mtu );
  o->mcaches [ idx ] = link->mcache;
  o->depths  [ idx ] = depth;
  o->cr_avail[ idx ] = ULONG_MAX;
  o->min_cr_avail    = ULONG_MAX;
  o->in_fseq[ 0 ]    = &o->in_fseq_value;
  *o->stem = (fd_stem_context_t) {
    .mcaches      = o->mcaches,
    .seqs         = o->seqs,
    .depths       = o->depths,
    .cr_avail     = o->cr_avail,
    .min_cr_avail = &o->min_cr_avail,
    .out_reliable = o->out_reliable,
    .in_fseq      = o->in_fseq,
  };
  return link;
}

static inline void
bam_fuzz_outs_delete( bam_fuzz_outs_t * o ) {
  for( ulong i=0UL; i<BAM_FUZZ_OUT_MAX; i++ ) {
    if( o->link[ i ].mcache ) bam_fuzz_link_delete( &o->link[ i ] );
  }
}

/* bam_fuzz_outs_{begin,end} bracket an adapter call to report what it
   published. */

static inline bam_fuzz_res_t
bam_fuzz_outs_begin( bam_fuzz_outs_t const * o ) {
  bam_fuzz_res_t res;
  for( ulong i=0UL; i<BAM_FUZZ_OUT_MAX; i++ ) res.out[ i ] = (bam_fuzz_range_t){ o->seqs[ i ], o->seqs[ i ] };
  return res;
}

static inline bam_fuzz_res_t
bam_fuzz_outs_end( bam_fuzz_outs_t const * o,
                   bam_fuzz_res_t          res ) {
  for( ulong i=0UL; i<BAM_FUZZ_OUT_MAX; i++ ) res.out[ i ].hi = o->seqs[ i ];
  return res;
}

typedef struct bam_fuzz_verify bam_fuzz_verify_t;
typedef struct bam_fuzz_dedup  bam_fuzz_dedup_t;
typedef struct bam_fuzz_resolv bam_fuzz_resolv_t;
typedef struct bam_fuzz_pack   bam_fuzz_pack_t;
typedef struct bam_fuzz_execle bam_fuzz_execle_t;

/* bam_fuzz_<stage>_new creates an adapter consuming in and copies its
   output links (indexed as above, else 0) to out. */

bam_fuzz_verify_t * bam_fuzz_verify_new( fd_wksp_t * wksp, bam_fuzz_link_t const * in, bam_fuzz_link_t out[ BAM_FUZZ_OUT_MAX ] );
bam_fuzz_dedup_t *  bam_fuzz_dedup_new ( fd_wksp_t * wksp, bam_fuzz_link_t const * in, bam_fuzz_link_t out[ BAM_FUZZ_OUT_MAX ] );
bam_fuzz_resolv_t * bam_fuzz_resolv_new( fd_wksp_t * wksp, bam_fuzz_link_t const * in, bam_fuzz_link_t out[ BAM_FUZZ_OUT_MAX ] );
bam_fuzz_pack_t *   bam_fuzz_pack_new  ( fd_wksp_t * wksp, bam_fuzz_link_t const * in, bam_fuzz_link_t out[ BAM_FUZZ_OUT_MAX ],
                                         ulong ** execle_busy_fseq );
bam_fuzz_execle_t * bam_fuzz_execle_new( fd_wksp_t * wksp, bam_fuzz_link_t const * in, bam_fuzz_link_t out[ BAM_FUZZ_OUT_MAX ],
                                         ulong * busy_fseq );

void bam_fuzz_verify_delete( bam_fuzz_verify_t * h );
void bam_fuzz_dedup_delete ( bam_fuzz_dedup_t *  h );
void bam_fuzz_resolv_delete( bam_fuzz_resolv_t * h );
void bam_fuzz_pack_delete  ( bam_fuzz_pack_t *   h );
void bam_fuzz_execle_delete( bam_fuzz_execle_t * h );

bam_fuzz_res_t bam_fuzz_verify_frag( bam_fuzz_verify_t * h, fd_frag_meta_t const * meta, ulong seq );
bam_fuzz_res_t bam_fuzz_dedup_frag ( bam_fuzz_dedup_t *  h, fd_frag_meta_t const * meta, ulong seq );
bam_fuzz_res_t bam_fuzz_resolv_frag( bam_fuzz_resolv_t * h, fd_frag_meta_t const * meta, ulong seq );
bam_fuzz_res_t bam_fuzz_pack_frag  ( bam_fuzz_pack_t *   h, fd_frag_meta_t const * meta, ulong seq, long now_ns );
bam_fuzz_res_t bam_fuzz_execle_frag( bam_fuzz_execle_t * h, fd_frag_meta_t const * meta, ulong seq );

bam_fuzz_res_t bam_fuzz_resolv_credit( bam_fuzz_resolv_t * h );
bam_fuzz_res_t bam_fuzz_pack_credit  ( bam_fuzz_pack_t *   h, long now_ns );

/* Slot transitions: execle builds (or reuses) a bank for slot and
   returns it, then resolv and pack follow it.  slot==ULONG_MAX ends
   leadership. */

void
bam_fuzz_execle_prepare_slot( bam_fuzz_execle_t * h,
                              ulong               slot,
                              fd_banks_t **       banks,
                              fd_accdb_t **       accdb,
                              void const **       bank,
                              ulong *             bank_idx );

void
bam_fuzz_resolv_prepare_slot( bam_fuzz_resolv_t * h,
                              ulong               slot,
                              fd_banks_t *        banks,
                              fd_accdb_t *        accdb,
                              fd_bank_t *         bank );

bam_fuzz_res_t
bam_fuzz_pack_set_leader_slot( bam_fuzz_pack_t * h,
                               ulong             slot,
                               void const *      bank,
                               ulong             bank_idx,
                               long              now_ns );

#endif /* HEADER_fd_src_disco_bam_fuzz_bam_pipeline_stage_h */
