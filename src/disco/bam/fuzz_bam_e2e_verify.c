#define FD_TILE_TEST 1
#define fd_tile_verify bam_fuzz_verify_tile_unused
#include "../verify/fd_verify_tile.c"
#undef fd_tile_verify

#include "fuzz_bam_e2e_verify.h"

static void (* const bam_fuzz_verify_init_refs[])( fd_topo_t *, fd_topo_tile_t * ) __attribute__((unused)) = {
  privileged_init,
  unprivileged_init,
};

static ulong (* const bam_fuzz_verify_policy_refs[])( fd_topo_t const *, fd_topo_tile_t const *, ulong, struct sock_filter * ) __attribute__((unused)) = {
  populate_allowed_seccomp,
};

static ulong (* const bam_fuzz_verify_fds_refs[])( fd_topo_t const *, fd_topo_tile_t const *, ulong, int * ) __attribute__((unused)) = {
  populate_allowed_fds,
};

#define BAM_FUZZ_VERIFY_IN_BAM_IDX       (0UL)
#define BAM_FUZZ_VERIFY_OUT_DEDUP_IDX    (0UL)
#define BAM_FUZZ_VERIFY_OUT_BANK_BAM_IDX (1UL)
#define BAM_FUZZ_VERIFY_OUT_CNT          (2UL)
#define BAM_FUZZ_VERIFY_MCACHE_DEPTH     (256UL)
#define BAM_FUZZ_VERIFY_TCACHE_DEPTH     (128UL)

struct bam_fuzz_verify {
  fd_wksp_t * wksp;

  fd_verify_ctx_t * ctx;

  void * storage_mem;

  void * tcache_mem;
  fd_tcache_t * tcache;

  void * sha_mem[ FD_TXN_ACTUAL_SIG_MAX ];

  void *           verify_mcache_mem;
  fd_frag_meta_t * verify_mcache;
  void *           verify_dcache_mem;
  uchar *          verify_dcache;

  void *           bank_bam_mcache_mem;
  fd_frag_meta_t * bank_bam_mcache;
  void *           bank_bam_dcache_mem;
  uchar *          bank_bam_dcache;

  fd_stem_context_t stem[1];
  fd_frag_meta_t *  stem_mcaches[ BAM_FUZZ_VERIFY_OUT_CNT ];
  ulong             stem_seqs[ BAM_FUZZ_VERIFY_OUT_CNT ];
  ulong             stem_depths[ BAM_FUZZ_VERIFY_OUT_CNT ];
  ulong             stem_cr_avail[ BAM_FUZZ_VERIFY_OUT_CNT ];
  ulong             stem_min_cr_avail[1];
  int               stem_out_reliable[ BAM_FUZZ_VERIFY_OUT_CNT ];
};

bam_fuzz_verify_t *
bam_fuzz_verify_new( fd_wksp_t * wksp,
                     fd_wksp_t * in_mem,
                     ulong       in_chunk0,
                     ulong       in_wmark,
                     bam_fuzz_link_t * out,
                     bam_fuzz_link_t * bank_bam_out ) {
  bam_fuzz_verify_t * h = fd_wksp_alloc_laddr( wksp, alignof(bam_fuzz_verify_t), sizeof(bam_fuzz_verify_t), 1UL );
  FD_TEST( h );
  fd_memset( h, 0, sizeof(*h) );
  h->wksp = wksp;

  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_verify_ctx_t), sizeof(fd_verify_ctx_t) );
  l = FD_LAYOUT_APPEND( l, fd_tcache_align(),        fd_tcache_footprint( BAM_FUZZ_VERIFY_TCACHE_DEPTH, 0UL ) );
  for( ulong i=0UL; i<FD_TXN_ACTUAL_SIG_MAX; i++ )
    l = FD_LAYOUT_APPEND( l, fd_sha512_align(), fd_sha512_footprint() );
  l = FD_LAYOUT_APPEND( l, fd_mcache_align(), fd_mcache_footprint( BAM_FUZZ_VERIFY_MCACHE_DEPTH, 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_dcache_align(), fd_dcache_footprint( fd_dcache_req_data_sz( FD_TPU_PARSED_MTU, BAM_FUZZ_VERIFY_MCACHE_DEPTH, 1UL, 1 ), 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_mcache_align(), fd_mcache_footprint( BAM_FUZZ_VERIFY_MCACHE_DEPTH, 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_dcache_align(), fd_dcache_footprint( fd_dcache_req_data_sz( sizeof(fd_bam_bundle_result_t), BAM_FUZZ_VERIFY_MCACHE_DEPTH, 1UL, 1 ), 0UL ) );
  ulong storage_footprint = FD_LAYOUT_FINI( l, fd_dcache_align() );
  h->storage_mem = fd_wksp_alloc_laddr( wksp, fd_dcache_align(), storage_footprint, 1UL );
  FD_TEST( h->storage_mem );

  FD_SCRATCH_ALLOC_INIT( alloc, h->storage_mem );
  ulong verify_data_sz   = fd_dcache_req_data_sz( FD_TPU_PARSED_MTU, BAM_FUZZ_VERIFY_MCACHE_DEPTH, 1UL, 1 );
  ulong bank_bam_data_sz = fd_dcache_req_data_sz( sizeof(fd_bam_bundle_result_t), BAM_FUZZ_VERIFY_MCACHE_DEPTH, 1UL, 1 );

  h->ctx = FD_SCRATCH_ALLOC_APPEND( alloc, alignof(fd_verify_ctx_t), sizeof(fd_verify_ctx_t) );
  FD_TEST( h->ctx );
  fd_memset( h->ctx, 0, sizeof(fd_verify_ctx_t) );

  h->tcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_tcache_align(), fd_tcache_footprint( BAM_FUZZ_VERIFY_TCACHE_DEPTH, 0UL ) );
  h->tcache = fd_tcache_join( fd_tcache_new( h->tcache_mem, BAM_FUZZ_VERIFY_TCACHE_DEPTH, 0UL ) );
  FD_TEST( h->tcache );

  for( ulong i=0UL; i<FD_TXN_ACTUAL_SIG_MAX; i++ ) {
    h->sha_mem[ i ] = FD_SCRATCH_ALLOC_APPEND( alloc, fd_sha512_align(), fd_sha512_footprint() );
    h->ctx->sha[ i ] = fd_sha512_join( fd_sha512_new( h->sha_mem[ i ] ) );
    FD_TEST( h->ctx->sha[ i ] );
  }

  h->verify_mcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_mcache_align(), fd_mcache_footprint( BAM_FUZZ_VERIFY_MCACHE_DEPTH, 0UL ) );
  h->verify_mcache     = fd_mcache_join( fd_mcache_new( h->verify_mcache_mem, BAM_FUZZ_VERIFY_MCACHE_DEPTH, 0UL, 0UL ) );
  FD_TEST( h->verify_mcache );
  h->verify_dcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_dcache_align(), fd_dcache_footprint( verify_data_sz, 0UL ) );
  h->verify_dcache     = fd_dcache_join( fd_dcache_new( h->verify_dcache_mem, verify_data_sz, 0UL ) );
  FD_TEST( h->verify_dcache );

  h->bank_bam_mcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_mcache_align(), fd_mcache_footprint( BAM_FUZZ_VERIFY_MCACHE_DEPTH, 0UL ) );
  h->bank_bam_mcache     = fd_mcache_join( fd_mcache_new( h->bank_bam_mcache_mem, BAM_FUZZ_VERIFY_MCACHE_DEPTH, 0UL, 0UL ) );
  FD_TEST( h->bank_bam_mcache );
  h->bank_bam_dcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_dcache_align(), fd_dcache_footprint( bank_bam_data_sz, 0UL ) );
  h->bank_bam_dcache     = fd_dcache_join( fd_dcache_new( h->bank_bam_dcache_mem, bank_bam_data_sz, 0UL ) );
  FD_TEST( h->bank_bam_dcache );

  ulong storage_top = FD_SCRATCH_ALLOC_FINI( alloc, fd_dcache_align() );
  FD_TEST( storage_top <= (ulong)h->storage_mem + storage_footprint );

  h->stem_mcaches[ BAM_FUZZ_VERIFY_OUT_DEDUP_IDX    ] = h->verify_mcache;
  h->stem_mcaches[ BAM_FUZZ_VERIFY_OUT_BANK_BAM_IDX ] = h->bank_bam_mcache;
  for( ulong i=0UL; i<BAM_FUZZ_VERIFY_OUT_CNT; i++ ) {
    h->stem_depths[ i ] = BAM_FUZZ_VERIFY_MCACHE_DEPTH;
    h->stem_cr_avail[ i ] = ULONG_MAX;
    h->stem_out_reliable[ i ] = 0;
  }
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

  h->ctx->round_robin_cnt = 1UL;
  h->ctx->round_robin_idx = 0UL;
  h->ctx->in_kind[ BAM_FUZZ_VERIFY_IN_BAM_IDX ] = IN_KIND_BAM;
  h->ctx->in[ BAM_FUZZ_VERIFY_IN_BAM_IDX ] = (fd_verify_in_ctx_t) {
    .mem    = in_mem,
    .chunk0 = in_chunk0,
    .wmark  = in_wmark,
  };
  h->ctx->out_mem    = (fd_wksp_t *)h->verify_dcache;
  h->ctx->out_chunk0 = fd_dcache_compact_chunk0( h->verify_dcache, h->verify_dcache );
  h->ctx->out_wmark  = fd_dcache_compact_wmark ( h->verify_dcache, h->verify_dcache, FD_TPU_PARSED_MTU );
  h->ctx->out_chunk  = h->ctx->out_chunk0;

  h->ctx->bam_result_out_mem    = (fd_wksp_t *)h->bank_bam_dcache;
  h->ctx->bam_result_out_idx    = BAM_FUZZ_VERIFY_OUT_BANK_BAM_IDX;
  h->ctx->bam_result_out_chunk0 = fd_dcache_compact_chunk0( h->bank_bam_dcache, h->bank_bam_dcache );
  h->ctx->bam_result_out_wmark  = fd_dcache_compact_wmark ( h->bank_bam_dcache, h->bank_bam_dcache, sizeof(fd_bam_bundle_result_t) );
  h->ctx->bam_result_out_chunk  = h->ctx->bam_result_out_chunk0;
  h->ctx->hashmap_seed = 0xBEEFUL;
  h->ctx->tcache_depth   = fd_tcache_depth       ( h->tcache );
  h->ctx->tcache_map_cnt = fd_tcache_map_cnt     ( h->tcache );
  h->ctx->tcache_sync    = fd_tcache_oldest_laddr( h->tcache );
  h->ctx->tcache_ring    = fd_tcache_ring_laddr  ( h->tcache );
  h->ctx->tcache_map     = fd_tcache_map_laddr   ( h->tcache );

  if( FD_LIKELY( out ) ) {
    *out = (bam_fuzz_link_t) {
      .mem    = (fd_wksp_t *)h->verify_dcache,
      .mcache = h->verify_mcache,
      .chunk0 = h->ctx->out_chunk0,
      .wmark  = h->ctx->out_wmark,
      .depth  = BAM_FUZZ_VERIFY_MCACHE_DEPTH,
    };
  }
  if( FD_LIKELY( bank_bam_out ) ) {
    *bank_bam_out = (bam_fuzz_link_t) {
      .mem    = (fd_wksp_t *)h->bank_bam_dcache,
      .mcache = h->bank_bam_mcache,
      .chunk0 = h->ctx->bam_result_out_chunk0,
      .wmark  = h->ctx->bam_result_out_wmark,
      .depth  = BAM_FUZZ_VERIFY_MCACHE_DEPTH,
    };
  }

  return h;
}

void
bam_fuzz_verify_delete( bam_fuzz_verify_t * h ) {
  if( FD_UNLIKELY( !h ) ) return;

  for( ulong i=0UL; i<FD_TXN_ACTUAL_SIG_MAX; i++ ) {
    if( h->ctx && h->ctx->sha[ i ] ) {
      void * mem = fd_sha512_delete( fd_sha512_leave( h->ctx->sha[ i ] ) );
      FD_TEST( mem==h->sha_mem[ i ] );
      h->ctx->sha[ i ] = NULL;
    }
  }
  if( h->tcache ) {
    void * mem = fd_tcache_delete( fd_tcache_leave( h->tcache ) );
    FD_TEST( mem==h->tcache_mem );
    h->tcache = NULL;
  }

  bam_fuzz_delete_mcache( &h->verify_mcache,   h->verify_mcache_mem   );
  bam_fuzz_delete_dcache( &h->verify_dcache,   h->verify_dcache_mem   );
  bam_fuzz_delete_mcache( &h->bank_bam_mcache, h->bank_bam_mcache_mem );
  bam_fuzz_delete_dcache( &h->bank_bam_dcache, h->bank_bam_dcache_mem );

  if( h->storage_mem ) {
    fd_wksp_free_laddr( h->storage_mem );
    h->storage_mem = NULL;
  }
  fd_wksp_free_laddr( h );
}

bam_fuzz_verify_result_t
bam_fuzz_verify_frag( bam_fuzz_verify_t * h,
                      fd_frag_meta_t const * meta,
                      ulong seq ) {
  bam_fuzz_verify_result_t res = {
    .verify_before   = h->stem_seqs[ BAM_FUZZ_VERIFY_OUT_DEDUP_IDX ],
    .verify_after    = h->stem_seqs[ BAM_FUZZ_VERIFY_OUT_DEDUP_IDX ],
    .bank_bam_before = h->stem_seqs[ BAM_FUZZ_VERIFY_OUT_BANK_BAM_IDX ],
    .bank_bam_after  = h->stem_seqs[ BAM_FUZZ_VERIFY_OUT_BANK_BAM_IDX ],
  };

  int filtered = before_frag( h->ctx, BAM_FUZZ_VERIFY_IN_BAM_IDX, seq, meta->sig );
  FD_TEST( !filtered );
  during_frag( h->ctx, BAM_FUZZ_VERIFY_IN_BAM_IDX, seq, meta->sig, meta->chunk, meta->sz, meta->ctl );
  after_frag( h->ctx, BAM_FUZZ_VERIFY_IN_BAM_IDX, seq, meta->sig, meta->sz, meta->tsorig, meta->tspub, h->stem );

  res.verify_after   = h->stem_seqs[ BAM_FUZZ_VERIFY_OUT_DEDUP_IDX ];
  res.bank_bam_after = h->stem_seqs[ BAM_FUZZ_VERIFY_OUT_BANK_BAM_IDX ];
  return res;
}
