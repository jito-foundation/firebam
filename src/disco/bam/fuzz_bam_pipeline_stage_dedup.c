#define FD_TILE_TEST 1
#define fd_tile_dedup bam_fuzz_dedup_tile_unused
#include "../dedup/fd_dedup_tile.c"
#undef fd_tile_dedup

#include "fuzz_bam_pipeline_stage_dedup.h"

static void (* const bam_fuzz_dedup_init_refs[])( fd_topo_t const *, fd_topo_tile_t const * ) __attribute__((unused)) = {
  privileged_init,
  unprivileged_init,
};

static ulong (* const bam_fuzz_dedup_policy_refs[])( fd_topo_t const *, fd_topo_tile_t const *, ulong, struct sock_filter * ) __attribute__((unused)) = {
  populate_allowed_seccomp,
};

static ulong (* const bam_fuzz_dedup_fds_refs[])( fd_topo_t const *, fd_topo_tile_t const *, ulong, int * ) __attribute__((unused)) = {
  populate_allowed_fds,
};

#define BAM_FUZZ_DEDUP_IN_VERIFY_IDX (0UL)
#define BAM_FUZZ_DEDUP_OUT_IDX       (0UL)
#define BAM_FUZZ_DEDUP_OUT_CNT       (1UL)
#define BAM_FUZZ_DEDUP_MCACHE_DEPTH  (256UL)
#define BAM_FUZZ_DEDUP_TCACHE_DEPTH  (128UL)

struct bam_fuzz_dedup {
  fd_wksp_t * wksp;

  fd_dedup_ctx_t * ctx;

  void * storage_mem;

  void * tcache_mem;
  fd_tcache_t * tcache;

  void *           dedup_mcache_mem;
  fd_frag_meta_t * dedup_mcache;
  void *           dedup_dcache_mem;
  uchar *          dedup_dcache;

  fd_stem_context_t stem[1];
  fd_frag_meta_t *  stem_mcaches[ BAM_FUZZ_DEDUP_OUT_CNT ];
  ulong             stem_seqs[ BAM_FUZZ_DEDUP_OUT_CNT ];
  ulong             stem_depths[ BAM_FUZZ_DEDUP_OUT_CNT ];
  ulong             stem_cr_avail[ BAM_FUZZ_DEDUP_OUT_CNT ];
  ulong             stem_min_cr_avail[1];
  int               stem_out_reliable[ BAM_FUZZ_DEDUP_OUT_CNT ];
};

bam_fuzz_dedup_t *
bam_fuzz_dedup_new( fd_wksp_t * wksp,
                    fd_wksp_t * in_mem,
                    ulong       in_chunk0,
                    ulong       in_wmark,
                    bam_fuzz_link_t * out ) {
  bam_fuzz_dedup_t * h = fd_wksp_alloc_laddr( wksp, alignof(bam_fuzz_dedup_t), sizeof(bam_fuzz_dedup_t), 1UL );
  FD_TEST( h );
  fd_memset( h, 0, sizeof(*h) );
  h->wksp = wksp;

  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_dedup_ctx_t), sizeof(fd_dedup_ctx_t) );
  l = FD_LAYOUT_APPEND( l, fd_tcache_align(),       fd_tcache_footprint( BAM_FUZZ_DEDUP_TCACHE_DEPTH, 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_mcache_align(),       fd_mcache_footprint( BAM_FUZZ_DEDUP_MCACHE_DEPTH, 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_dcache_align(),       fd_dcache_footprint( fd_dcache_req_data_sz( FD_TPU_PARSED_MTU, BAM_FUZZ_DEDUP_MCACHE_DEPTH, 1UL, 1 ), 0UL ) );
  ulong storage_footprint = FD_LAYOUT_FINI( l, fd_dcache_align() );
  h->storage_mem = fd_wksp_alloc_laddr( wksp, fd_dcache_align(), storage_footprint, 1UL );
  FD_TEST( h->storage_mem );

  FD_SCRATCH_ALLOC_INIT( alloc, h->storage_mem );
  ulong dedup_data_sz = fd_dcache_req_data_sz( FD_TPU_PARSED_MTU, BAM_FUZZ_DEDUP_MCACHE_DEPTH, 1UL, 1 );

  h->ctx = FD_SCRATCH_ALLOC_APPEND( alloc, alignof(fd_dedup_ctx_t), sizeof(fd_dedup_ctx_t) );
  FD_TEST( h->ctx );
  fd_memset( h->ctx, 0, sizeof(fd_dedup_ctx_t) );

  h->tcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_tcache_align(), fd_tcache_footprint( BAM_FUZZ_DEDUP_TCACHE_DEPTH, 0UL ) );
  h->tcache = fd_tcache_join( fd_tcache_new( h->tcache_mem, BAM_FUZZ_DEDUP_TCACHE_DEPTH, 0UL ) );
  FD_TEST( h->tcache );

  h->dedup_mcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_mcache_align(), fd_mcache_footprint( BAM_FUZZ_DEDUP_MCACHE_DEPTH, 0UL ) );
  h->dedup_mcache = fd_mcache_join( fd_mcache_new( h->dedup_mcache_mem, BAM_FUZZ_DEDUP_MCACHE_DEPTH, 0UL, 0UL ) );
  FD_TEST( h->dedup_mcache );
  h->dedup_dcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_dcache_align(), fd_dcache_footprint( dedup_data_sz, 0UL ) );
  h->dedup_dcache = fd_dcache_join( fd_dcache_new( h->dedup_dcache_mem, dedup_data_sz, 0UL ) );
  FD_TEST( h->dedup_dcache );

  ulong storage_top = FD_SCRATCH_ALLOC_FINI( alloc, fd_dcache_align() );
  FD_TEST( storage_top <= (ulong)h->storage_mem + storage_footprint );

  h->stem_mcaches[ BAM_FUZZ_DEDUP_OUT_IDX ] = h->dedup_mcache;
  h->stem_depths[ BAM_FUZZ_DEDUP_OUT_IDX ] = BAM_FUZZ_DEDUP_MCACHE_DEPTH;
  h->stem_cr_avail[ BAM_FUZZ_DEDUP_OUT_IDX ] = ULONG_MAX;
  h->stem_min_cr_avail[0] = ULONG_MAX;
  *h->stem = (fd_stem_context_t) {
    .mcaches             = h->stem_mcaches,
    .seqs                = h->stem_seqs,
    .depths              = h->stem_depths,
    .cr_avail            = h->stem_cr_avail,
    .min_cr_avail        = h->stem_min_cr_avail,
    .cr_decrement_amount = 0UL,
    .out_reliable        = h->stem_out_reliable,
  };

  h->ctx->in_kind[ BAM_FUZZ_DEDUP_IN_VERIFY_IDX ] = IN_KIND_VERIFY;
  h->ctx->in[ BAM_FUZZ_DEDUP_IN_VERIFY_IDX ] = (fd_dedup_in_ctx_t) {
    .mem    = in_mem,
    .chunk0 = in_chunk0,
    .wmark  = in_wmark,
    .mtu    = FD_TPU_PARSED_MTU,
  };
  h->ctx->out_mem    = (fd_wksp_t *)h->dedup_dcache;
  h->ctx->out_chunk0 = fd_dcache_compact_chunk0( h->dedup_dcache, h->dedup_dcache );
  h->ctx->out_wmark  = fd_dcache_compact_wmark ( h->dedup_dcache, h->dedup_dcache, FD_TPU_PARSED_MTU );
  h->ctx->out_chunk  = h->ctx->out_chunk0;
  h->ctx->hashmap_seed = 0xD00DUL;
  h->ctx->tcache_depth   = fd_tcache_depth       ( h->tcache );
  h->ctx->tcache_map_cnt = fd_tcache_map_cnt     ( h->tcache );
  h->ctx->tcache_sync    = fd_tcache_oldest_laddr( h->tcache );
  h->ctx->tcache_ring    = fd_tcache_ring_laddr  ( h->tcache );
  h->ctx->tcache_map     = fd_tcache_map_laddr   ( h->tcache );

  if( FD_LIKELY( out ) ) {
    *out = (bam_fuzz_link_t) {
      .mem    = (fd_wksp_t *)h->dedup_dcache,
      .mcache = h->dedup_mcache,
      .chunk0 = h->ctx->out_chunk0,
      .wmark  = h->ctx->out_wmark,
      .depth  = BAM_FUZZ_DEDUP_MCACHE_DEPTH,
    };
  }

  return h;
}

void
bam_fuzz_dedup_delete( bam_fuzz_dedup_t * h ) {
  if( FD_UNLIKELY( !h ) ) return;

  if( h->tcache ) {
    void * mem = fd_tcache_delete( fd_tcache_leave( h->tcache ) );
    FD_TEST( mem==h->tcache_mem );
    h->tcache = NULL;
  }
  bam_fuzz_delete_mcache( &h->dedup_mcache, h->dedup_mcache_mem );
  bam_fuzz_delete_dcache( &h->dedup_dcache, h->dedup_dcache_mem );
  if( h->storage_mem ) {
    fd_wksp_free_laddr( h->storage_mem );
    h->storage_mem = NULL;
  }
  fd_wksp_free_laddr( h );
}

bam_fuzz_dedup_result_t
bam_fuzz_dedup_frag( bam_fuzz_dedup_t * h,
                     fd_frag_meta_t const * meta,
                     ulong seq ) {
  bam_fuzz_dedup_result_t res = {
    .dedup_before = h->stem_seqs[ BAM_FUZZ_DEDUP_OUT_IDX ],
    .dedup_after  = h->stem_seqs[ BAM_FUZZ_DEDUP_OUT_IDX ],
  };

  fd_txn_m_t const * txnm =
      (fd_txn_m_t const *)fd_chunk_to_laddr_const( h->ctx->in[ BAM_FUZZ_DEDUP_IN_VERIFY_IDX ].mem,
                                                   meta->chunk );
  if( FD_UNLIKELY( txnm->block_engine.bundle_id &&
                   !txnm->bam.batch_idx &&
                   h->ctx->bundle_id==txnm->block_engine.bundle_id &&
                   h->ctx->bundle_idx ) ) {
    h->ctx->bundle_id  = 0UL;
    h->ctx->bundle_idx = 0UL;
  }

  during_frag( h->ctx, BAM_FUZZ_DEDUP_IN_VERIFY_IDX, seq, meta->sig, meta->chunk, meta->sz, meta->ctl );
  after_frag( h->ctx, BAM_FUZZ_DEDUP_IN_VERIFY_IDX, seq, meta->sig, meta->sz, meta->tsorig, meta->tspub, h->stem );

  res.dedup_after = h->stem_seqs[ BAM_FUZZ_DEDUP_OUT_IDX ];
  return res;
}
