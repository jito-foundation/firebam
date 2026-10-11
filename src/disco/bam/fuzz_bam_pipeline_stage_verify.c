#define fd_tile_verify bam_fuzz_verify_tile_unused
#include "../verify/fd_verify_tile.c"
#undef fd_tile_verify

#include "fuzz_bam_pipeline_stage.h"

#define BAM_FUZZ_VERIFY_IN_BAM_IDX   (0UL)
#define BAM_FUZZ_VERIFY_MCACHE_DEPTH (256UL)
#define BAM_FUZZ_VERIFY_TCACHE_DEPTH (128UL)

struct bam_fuzz_verify {
  fd_verify_ctx_t ctx[1];
  fd_tcache_t *   tcache;
  bam_fuzz_outs_t outs;
};

bam_fuzz_verify_t *
bam_fuzz_verify_new( fd_wksp_t *             wksp,
                     bam_fuzz_link_t const * in,
                     bam_fuzz_link_t         out[ BAM_FUZZ_OUT_MAX ] ) {
  bam_fuzz_verify_t * h = fd_wksp_alloc_laddr( wksp, alignof(bam_fuzz_verify_t), sizeof(bam_fuzz_verify_t), 1UL );
  FD_TEST( h );
  fd_memset( h, 0, sizeof(*h) );
  fd_verify_ctx_t * ctx = h->ctx;

  h->tcache = fd_tcache_join( fd_tcache_new( fd_wksp_alloc_laddr( wksp, fd_tcache_align(), fd_tcache_footprint( BAM_FUZZ_VERIFY_TCACHE_DEPTH, 0UL ), 1UL ),
                                             BAM_FUZZ_VERIFY_TCACHE_DEPTH, 0UL ) );
  FD_TEST( h->tcache );
  for( ulong i=0UL; i<FD_TXN_SIG_MAX; i++ ) {
    ctx->sha[ i ] = fd_sha512_join( fd_sha512_new( fd_wksp_alloc_laddr( wksp, fd_sha512_align(), fd_sha512_footprint(), 1UL ) ) );
    FD_TEST( ctx->sha[ i ] );
  }
  ctx->ed25519_cache = fd_ed25519_cache_join( fd_ed25519_cache_new(
      fd_wksp_alloc_laddr( wksp, fd_ed25519_cache_align(), fd_ed25519_cache_footprint( FD_VERIFY_ED25519_CACHE_ENT_CNT ), 1UL ),
      FD_VERIFY_ED25519_CACHE_ENT_CNT, 0xBEEFUL ) );
  FD_TEST( ctx->ed25519_cache );

  bam_fuzz_link_t const * o = bam_fuzz_outs_add( &h->outs, 0UL, wksp, BAM_FUZZ_VERIFY_MCACHE_DEPTH, FD_TPU_PARSED_MTU );

  ctx->round_robin_cnt = 1UL;
  ctx->round_robin_idx = 0UL;
  ctx->in_kind[ BAM_FUZZ_VERIFY_IN_BAM_IDX ] = IN_KIND_BAM;
  ctx->in     [ BAM_FUZZ_VERIFY_IN_BAM_IDX ] = (fd_verify_in_ctx_t){ .mem = in->mem, .chunk0 = in->chunk0, .wmark = in->wmark };
  ctx->out_mem    = o->mem;
  ctx->out_chunk0 = o->chunk0;
  ctx->out_wmark  = o->wmark;
  ctx->out_chunk  = o->chunk0;

  ctx->hashmap_seed   = 0xBEEFUL;
  ctx->tcache_depth   = fd_tcache_depth       ( h->tcache );
  ctx->tcache_map_cnt = fd_tcache_map_cnt     ( h->tcache );
  ctx->tcache_sync    = fd_tcache_oldest_laddr( h->tcache );
  ctx->tcache_ring    = fd_tcache_ring_laddr  ( h->tcache );
  ctx->tcache_map     = fd_tcache_map_laddr   ( h->tcache );

  fd_memcpy( out, h->outs.link, sizeof(h->outs.link) );
  return h;
}

void
bam_fuzz_verify_delete( bam_fuzz_verify_t * h ) {
  for( ulong i=0UL; i<FD_TXN_SIG_MAX; i++ ) fd_wksp_free_laddr( fd_sha512_delete( fd_sha512_leave( h->ctx->sha[ i ] ) ) );
  fd_wksp_free_laddr( fd_tcache_delete( fd_tcache_leave( h->tcache ) ) );
  fd_wksp_free_laddr( fd_ed25519_cache_delete( fd_ed25519_cache_leave( h->ctx->ed25519_cache ) ) );
  bam_fuzz_outs_delete( &h->outs );
  fd_wksp_free_laddr( h );
}

bam_fuzz_res_t
bam_fuzz_verify_frag( bam_fuzz_verify_t *    h,
                      fd_frag_meta_t const * meta,
                      ulong                  seq ) {
  bam_fuzz_res_t res = bam_fuzz_outs_begin( &h->outs );
  FD_TEST( !before_frag( h->ctx, BAM_FUZZ_VERIFY_IN_BAM_IDX, seq, meta->sig ) );
  during_frag( h->ctx, BAM_FUZZ_VERIFY_IN_BAM_IDX, seq, meta->sig, meta->chunk, meta->sz, meta->ctl );
  after_frag( h->ctx, BAM_FUZZ_VERIFY_IN_BAM_IDX, seq, meta->sig, meta->sz, meta->tsorig, meta->tspub, h->outs.stem );
  return bam_fuzz_outs_end( &h->outs, res );
}
